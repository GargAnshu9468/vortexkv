package persistence

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RDBMagicHeader = "REDIS0009"

	OpcodeAUX          byte = 0xFA
	OpcodeRESIZEDB     byte = 0xFB
	OpcodeEXPIRETIMEMS byte = 0xFC
	OpcodeEXPIRETIME   byte = 0xFD
	OpcodeSELECTDB     byte = 0xFE
	OpcodeEOF          byte = 0xFF

	RDBTypeString byte = 0
	RDBTypeList   byte = 1
	RDBTypeSet    byte = 2
	RDBTypeZSet   byte = 3
	RDBTypeHash   byte = 4
	RDBTypeStream byte = 6
	RDBTypeVector byte = 240
)

var (
	ErrInvalidRDBHeader   = errors.New("invalid RDB header: expected REDIS magic prefix")
	ErrCRCMismatch        = errors.New("RDB checksum mismatch (file may be corrupted)")
	ErrBgSaveInProgress   = errors.New("Background save already in progress")
	ErrUnsupportedRDBType = errors.New("unsupported RDB value type")
)

type ZSetItem struct {
	Score  float64
	Member string
}

type StreamItem struct {
	ID     string
	Fields map[string]string
}

type VectorItem struct {
	Dim     int
	Vectors map[string][]float32
}

type RDBEntry struct {
	Key       string
	Type      byte
	ExpiresAt int64 // Milliseconds timestamp, 0 if no expiration
	Value     any
}

// SaveRDB serializes database entries to an RDB snapshot file with CRC64 checksum
func SaveRDB(path string, entries []RDBEntry, aux map[string]string) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	crc := NewCRC64()
	mw := io.MultiWriter(f, crc)
	bw := bufio.NewWriterSize(mw, 64*1024)

	// 1. Magic Header
	if _, err := bw.WriteString(RDBMagicHeader); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// 2. Aux metadata fields
	for k, v := range aux {
		_ = bw.WriteByte(OpcodeAUX)
		_ = writeString(bw, k)
		_ = writeString(bw, v)
	}

	// 3. Database selector: DB 0
	_ = bw.WriteByte(OpcodeSELECTDB)
	_ = writeVarint(bw, 0)

	// 4. RESIZEDB: total entries and expire count
	var expireCount uint64
	now := time.Now().UnixMilli()
	validEntries := make([]RDBEntry, 0, len(entries))
	for _, e := range entries {
		if e.ExpiresAt > 0 && e.ExpiresAt <= now {
			continue
		}
		if e.ExpiresAt > 0 {
			expireCount++
		}
		validEntries = append(validEntries, e)
	}

	_ = bw.WriteByte(OpcodeRESIZEDB)
	_ = writeVarint(bw, uint64(len(validEntries)))
	_ = writeVarint(bw, expireCount)

	// 5. Encode each key-value entry
	for _, entry := range validEntries {
		if entry.ExpiresAt > 0 {
			_ = bw.WriteByte(OpcodeEXPIRETIMEMS)
			var b8 [8]byte
			binary.LittleEndian.PutUint64(b8[:], uint64(entry.ExpiresAt))
			_, _ = bw.Write(b8[:])
		}

		_ = bw.WriteByte(entry.Type)
		_ = writeString(bw, entry.Key)

		if err := encodeValue(bw, entry.Type, entry.Value); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return err
		}
	}

	// 6. Opcode EOF
	_ = bw.WriteByte(OpcodeEOF)
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// 7. Write 8-byte little-endian CRC64 checksum (outside multiwriter)
	checksum := crc.Sum64()
	var crcBytes [8]byte
	binary.LittleEndian.PutUint64(crcBytes[:], checksum)
	if _, err := f.Write(crcBytes[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// Atomic replace
	return os.Rename(tmpPath, path)
}

// LoadRDB parses an RDB snapshot file and invokes handler for each restored entry
func LoadRDB(path string, handler func(entry RDBEntry) error) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() < 9+1+8 { // Header (9) + EOF (1) + CRC (8)
		return nil, errors.New("RDB file is too short to be valid")
	}

	crc := NewCRC64()
	payloadLimit := stat.Size() - 8
	lr := io.LimitReader(f, payloadLimit)
	tr := io.TeeReader(lr, crc)
	br := bufio.NewReaderSize(tr, 64*1024)

	// 1. Read header
	var header [9]byte
	if _, err := io.ReadFull(br, header[:]); err != nil {
		return nil, err
	}
	if string(header[:5]) != "REDIS" {
		return nil, ErrInvalidRDBHeader
	}

	aux := make(map[string]string)
	now := time.Now().UnixMilli()

	// 2. Read records loop
	for {
		b, err := br.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if b == OpcodeEOF {
			break
		}

		if b == OpcodeAUX {
			k, err := readString(br)
			if err != nil {
				return nil, err
			}
			v, err := readString(br)
			if err != nil {
				return nil, err
			}
			aux[k] = v
			continue
		}

		if b == OpcodeSELECTDB {
			_, _ = readVarint(br)
			continue
		}

		if b == OpcodeRESIZEDB {
			_, _ = readVarint(br)
			_, _ = readVarint(br)
			continue
		}

		var expiresAt int64
		if b == OpcodeEXPIRETIME {
			var b4 [4]byte
			if _, err := io.ReadFull(br, b4[:]); err != nil {
				return nil, err
			}
			sec := binary.LittleEndian.Uint32(b4[:])
			expiresAt = int64(sec) * 1000
			b, err = br.ReadByte()
			if err != nil {
				return nil, err
			}
		} else if b == OpcodeEXPIRETIMEMS {
			var b8 [8]byte
			if _, err := io.ReadFull(br, b8[:]); err != nil {
				return nil, err
			}
			expiresAt = int64(binary.LittleEndian.Uint64(b8[:]))
			b, err = br.ReadByte()
			if err != nil {
				return nil, err
			}
		}

		valType := b
		key, err := readString(br)
		if err != nil {
			return nil, err
		}

		val, err := decodeValue(br, valType)
		if err != nil {
			return nil, err
		}

		// Skip expired entries
		if expiresAt > 0 && expiresAt <= now {
			continue
		}

		if handler != nil {
			if err := handler(RDBEntry{
				Key:       key,
				Type:      valType,
				ExpiresAt: expiresAt,
				Value:     val,
			}); err != nil {
				return nil, err
			}
		}
	}

	// Drain any remaining bytes before EOF in limit reader
	_, _ = io.Copy(io.Discard, br)

	// Read 8-byte expected CRC from file
	var expectedCRCBytes [8]byte
	if _, err := io.ReadFull(f, expectedCRCBytes[:]); err != nil {
		return nil, fmt.Errorf("failed to read RDB CRC checksum: %w", err)
	}
	expectedCRC := binary.LittleEndian.Uint64(expectedCRCBytes[:])
	actualCRC := crc.Sum64()

	if expectedCRC != 0 && actualCRC != expectedCRC {
		return nil, ErrCRCMismatch
	}

	return aux, nil
}

// ---------------- Serialization Helpers ----------------

func encodeValue(w io.Writer, valType byte, val any) error {
	switch valType {
	case RDBTypeString:
		s, ok := val.(string)
		if !ok {
			return ErrUnsupportedRDBType
		}
		return writeString(w, s)

	case RDBTypeList, RDBTypeSet:
		items, ok := val.([]string)
		if !ok {
			return ErrUnsupportedRDBType
		}
		if err := writeVarint(w, uint64(len(items))); err != nil {
			return err
		}
		for _, item := range items {
			if err := writeString(w, item); err != nil {
				return err
			}
		}
		return nil

	case RDBTypeHash:
		fields, ok := val.(map[string]string)
		if !ok {
			return ErrUnsupportedRDBType
		}
		if err := writeVarint(w, uint64(len(fields))); err != nil {
			return err
		}
		for f, v := range fields {
			if err := writeString(w, f); err != nil {
				return err
			}
			if err := writeString(w, v); err != nil {
				return err
			}
		}
		return nil

	case RDBTypeZSet:
		items, ok := val.([]ZSetItem)
		if !ok {
			return ErrUnsupportedRDBType
		}
		if err := writeVarint(w, uint64(len(items))); err != nil {
			return err
		}
		for _, item := range items {
			var b8 [8]byte
			binary.LittleEndian.PutUint64(b8[:], math.Float64bits(item.Score))
			if _, err := w.Write(b8[:]); err != nil {
				return err
			}
			if err := writeString(w, item.Member); err != nil {
				return err
			}
		}
		return nil

	case RDBTypeStream:
		items, ok := val.([]StreamItem)
		if !ok {
			return ErrUnsupportedRDBType
		}
		if err := writeVarint(w, uint64(len(items))); err != nil {
			return err
		}
		for _, item := range items {
			if err := writeString(w, item.ID); err != nil {
				return err
			}
			if err := writeVarint(w, uint64(len(item.Fields))); err != nil {
				return err
			}
			for f, v := range item.Fields {
				if err := writeString(w, f); err != nil {
					return err
				}
				if err := writeString(w, v); err != nil {
					return err
				}
			}
		}
		return nil

	case RDBTypeVector:
		vecItem, ok := val.(VectorItem)
		if !ok {
			return ErrUnsupportedRDBType
		}
		if err := writeVarint(w, uint64(vecItem.Dim)); err != nil {
			return err
		}
		if err := writeVarint(w, uint64(len(vecItem.Vectors))); err != nil {
			return err
		}
		for id, vec := range vecItem.Vectors {
			if err := writeString(w, id); err != nil {
				return err
			}
			for _, f := range vec {
				var b4 [4]byte
				binary.LittleEndian.PutUint32(b4[:], math.Float32bits(f))
				if _, err := w.Write(b4[:]); err != nil {
					return err
				}
			}
		}
		return nil

	default:
		return ErrUnsupportedRDBType
	}
}

func decodeValue(r io.Reader, valType byte) (any, error) {
	switch valType {
	case RDBTypeString:
		return readString(r)

	case RDBTypeList, RDBTypeSet:
		n, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		items := make([]string, n)
		for i := uint64(0); i < n; i++ {
			item, err := readString(r)
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		return items, nil

	case RDBTypeHash:
		n, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		fields := make(map[string]string, n)
		for i := uint64(0); i < n; i++ {
			f, err := readString(r)
			if err != nil {
				return nil, err
			}
			v, err := readString(r)
			if err != nil {
				return nil, err
			}
			fields[f] = v
		}
		return fields, nil

	case RDBTypeZSet:
		n, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		items := make([]ZSetItem, n)
		for i := uint64(0); i < n; i++ {
			var b8 [8]byte
			if _, err := io.ReadFull(r, b8[:]); err != nil {
				return nil, err
			}
			score := math.Float64frombits(binary.LittleEndian.Uint64(b8[:]))
			member, err := readString(r)
			if err != nil {
				return nil, err
			}
			items[i] = ZSetItem{Score: score, Member: member}
		}
		return items, nil

	case RDBTypeStream:
		n, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		items := make([]StreamItem, n)
		for i := uint64(0); i < n; i++ {
			id, err := readString(r)
			if err != nil {
				return nil, err
			}
			fCount, err := readVarint(r)
			if err != nil {
				return nil, err
			}
			fields := make(map[string]string, fCount)
			for j := uint64(0); j < fCount; j++ {
				f, err := readString(r)
				if err != nil {
					return nil, err
				}
				v, err := readString(r)
				if err != nil {
					return nil, err
				}
				fields[f] = v
			}
			items[i] = StreamItem{ID: id, Fields: fields}
		}
		return items, nil

	case RDBTypeVector:
		dim, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		count, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		vectors := make(map[string][]float32, count)
		for i := uint64(0); i < count; i++ {
			id, err := readString(r)
			if err != nil {
				return nil, err
			}
			vec := make([]float32, dim)
			for d := uint64(0); d < dim; d++ {
				var b4 [4]byte
				if _, err := io.ReadFull(r, b4[:]); err != nil {
					return nil, err
				}
				vec[d] = math.Float32frombits(binary.LittleEndian.Uint32(b4[:]))
			}
			vectors[id] = vec
		}
		return VectorItem{Dim: int(dim), Vectors: vectors}, nil

	default:
		return nil, ErrUnsupportedRDBType
	}
}

func writeString(w io.Writer, s string) error {
	data := []byte(s)
	if err := writeVarint(w, uint64(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readString(r io.Reader) (string, error) {
	n, err := readVarint(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeVarint(w io.Writer, val uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], val)
	_, err := w.Write(buf[:n])
	return err
}

func readVarint(r io.Reader) (uint64, error) {
	if br, ok := r.(io.ByteReader); ok {
		return binary.ReadUvarint(br)
	}
	return binary.ReadUvarint(bufio.NewReader(r))
}

// ---------------- RDBManager ----------------

type RDBManager struct {
	mu               sync.RWMutex
	path             string
	lastSaveTime     int64
	lastBgSaveStatus string
	lastBgSaveSec    float64
	bgInProgress     atomic.Bool
}

func NewRDBManager(path string) *RDBManager {
	if path == "" {
		path = "dump.rdb"
	}
	return &RDBManager{
		path:             path,
		lastSaveTime:     time.Now().Unix(),
		lastBgSaveStatus: "ok",
	}
}

func (rm *RDBManager) Path() string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.path
}

func (rm *RDBManager) LastSave() int64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.lastSaveTime
}

func (rm *RDBManager) LastBgSaveStatus() string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.lastBgSaveStatus
}

func (rm *RDBManager) LastBgSaveSec() float64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.lastBgSaveSec
}

func (rm *RDBManager) IsBgSaveInProgress() bool {
	return rm.bgInProgress.Load()
}

func (rm *RDBManager) Save(entries []RDBEntry, aux map[string]string) error {
	start := time.Now()
	err := SaveRDB(rm.path, entries, aux)
	duration := time.Since(start).Seconds()

	rm.mu.Lock()
	defer rm.mu.Unlock()
	if err == nil {
		rm.lastSaveTime = time.Now().Unix()
		rm.lastBgSaveStatus = "ok"
		rm.lastBgSaveSec = duration
	} else {
		rm.lastBgSaveStatus = "err"
	}
	return err
}

func (rm *RDBManager) BgSave(getSnapshot func() ([]RDBEntry, map[string]string), onDone func(err error)) error {
	if !rm.bgInProgress.CompareAndSwap(false, true) {
		return ErrBgSaveInProgress
	}

	go func() {
		defer rm.bgInProgress.Store(false)
		entries, aux := getSnapshot()
		start := time.Now()
		err := SaveRDB(rm.path, entries, aux)
		duration := time.Since(start).Seconds()

		rm.mu.Lock()
		if err == nil {
			rm.lastSaveTime = time.Now().Unix()
			rm.lastBgSaveStatus = "ok"
			rm.lastBgSaveSec = duration
		} else {
			rm.lastBgSaveStatus = "err"
		}
		rm.mu.Unlock()

		if onDone != nil {
			onDone(err)
		}
	}()

	return nil
}
