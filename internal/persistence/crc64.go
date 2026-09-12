package persistence

import (
	"encoding/binary"
	"hash"
)

// Redis uses the CRC64-Jones polynomial: 0xad93d23594c935a9
const poly = 0xad93d23594c935a9

var crc64Table [256]uint64

func init() {
	for i := 0; i < 256; i++ {
		crc := uint64(i)
		for j := 0; j < 8; j++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		crc64Table[i] = crc
	}
}

// Digest represents a running 64-bit CRC calculation
type Digest struct {
	crc uint64
}

// NewCRC64 creates a new hash.Hash64 computing Redis CRC64-Jones checksum
func NewCRC64() hash.Hash64 {
	return &Digest{crc: 0}
}

func (d *Digest) Reset() {
	d.crc = 0
}

func (d *Digest) Size() int {
	return 8
}

func (d *Digest) BlockSize() int {
	return 1
}

func (d *Digest) Write(p []byte) (n int, err error) {
	d.crc = UpdateCRC64(d.crc, p)
	return len(p), nil
}

func (d *Digest) Sum64() uint64 {
	return d.crc
}

func (d *Digest) Sum(in []byte) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], d.crc)
	return append(in, b[:]...)
}

// UpdateCRC64 computes the running CRC64-Jones hash over byte slice p
func UpdateCRC64(crc uint64, p []byte) uint64 {
	for _, b := range p {
		crc = crc64Table[byte(crc)^b] ^ (crc >> 8)
	}
	return crc
}

// ChecksumCRC64 returns the complete CRC64-Jones checksum of data
func ChecksumCRC64(data []byte) uint64 {
	return UpdateCRC64(0, data)
}
