package datastruct

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidStreamID = errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
)

type StreamEntry struct {
	ID     string
	Fields map[string]string
	Order  []string // preserves field insertion order
}

type StreamID struct {
	Millis uint64
	Seq    uint64
}

func (s StreamID) String() string {
	return fmt.Sprintf("%d-%d", s.Millis, s.Seq)
}

func ParseStreamID(id string) (StreamID, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return StreamID{}, errors.New("invalid stream id format")
	}
	m, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, err
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, err
	}
	return StreamID{Millis: m, Seq: seq}, nil
}

type Stream struct {
	mu      sync.RWMutex
	entries []StreamEntry
	lastID  StreamID
}

func NewStream() *Stream {
	return &Stream{
		entries: make([]StreamEntry, 0),
	}
}

func (s *Stream) Add(idStr string, fields map[string]string, order []string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var sid StreamID
	if idStr == "*" {
		nowMilli := uint64(time.Now().UnixMilli())
		if nowMilli > s.lastID.Millis {
			sid = StreamID{Millis: nowMilli, Seq: 0}
		} else {
			sid = StreamID{Millis: s.lastID.Millis, Seq: s.lastID.Seq + 1}
		}
	} else if strings.HasSuffix(idStr, "-*") {
		prefix := strings.TrimSuffix(idStr, "-*")
		m, err := strconv.ParseUint(prefix, 10, 64)
		if err != nil {
			return "", err
		}
		if m < s.lastID.Millis {
			return "", ErrInvalidStreamID
		}
		if m == s.lastID.Millis {
			sid = StreamID{Millis: m, Seq: s.lastID.Seq + 1}
		} else {
			sid = StreamID{Millis: m, Seq: 0}
		}
	} else {
		parsed, err := ParseStreamID(idStr)
		if err != nil {
			return "", err
		}
		if len(s.entries) > 0 {
			if parsed.Millis < s.lastID.Millis || (parsed.Millis == s.lastID.Millis && parsed.Seq <= s.lastID.Seq) {
				return "", ErrInvalidStreamID
			}
		}
		sid = parsed
	}

	assignedID := sid.String()
	s.lastID = sid
	s.entries = append(s.entries, StreamEntry{
		ID:     assignedID,
		Fields: fields,
		Order:  order,
	})

	return assignedID, nil
}

func (s *Stream) Len() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.entries))
}

func (s *Stream) Range(start, end string, count int64) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.entries) == 0 {
		return []StreamEntry{}
	}

	var results []StreamEntry
	for _, entry := range s.entries {
		if start != "-" && start != "" && entry.ID < start {
			continue
		}
		if end != "+" && end != "" && entry.ID > end {
			break
		}
		results = append(results, entry)
		if count > 0 && int64(len(results)) >= count {
			break
		}
	}
	return results
}
