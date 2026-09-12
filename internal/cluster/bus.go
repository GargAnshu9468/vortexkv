package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// Magic bytes identifying VortexKV Cluster Bus packets
var BusMagic = [4]byte{'V', 'B', 'U', 'S'}

// Bus Packet Types
const (
	TypePing            uint8 = 1
	TypePong            uint8 = 2
	TypeFail            uint8 = 3
	TypeFailoverAuthReq uint8 = 4
	TypeFailoverAuthAck uint8 = 5
)

// Gossip Entry Flags
const (
	FlagMaster uint16 = 1 << 0
	FlagSlave  uint16 = 1 << 1
	FlagPFail  uint16 = 1 << 2
	FlagFail   uint16 = 1 << 3
)

// GossipEntry represents a peer node's health and state reported in a gossip packet.
type GossipEntry struct {
	NodeID   string
	IP       string
	Port     uint16
	BusPort  uint16
	Flags    uint16
	PingSent int64
	PongRecv int64
}

// BusPacket represents an in-flight cluster bus message.
type BusPacket struct {
	Type        uint8
	Epoch       uint64
	SenderID    string
	SenderIP    string
	Port        uint16
	BusPort     uint16
	Flags       uint16
	Slots       [2048]byte // 16384 bit slot assignment bitmap
	Gossip      []GossipEntry

	// Failover / Election fields
	TargetNodeID string // Used in TypeFail and TypeFailoverAuthReq
	ReqEpoch     uint64 // Used in TypeFailoverAuthReq and TypeFailoverAuthAck
}

// SlotsToBitmap compresses a 16384-boolean slot array into a 2048-byte bitmap.
func SlotsToBitmap(slots [16384]bool) [2048]byte {
	var bm [2048]byte
	for i := 0; i < 16384; i++ {
		if slots[i] {
			bm[i/8] |= 1 << (i % 8)
		}
	}
	return bm
}

// BitmapToSlots extracts a 16384-boolean slot array from a 2048-byte bitmap.
func BitmapToSlots(bm [2048]byte) [16384]bool {
	var slots [16384]bool
	for i := 0; i < 16384; i++ {
		if (bm[i/8] & (1 << (i % 8))) != 0 {
			slots[i] = true
		}
	}
	return slots
}

// WritePacket serializes and sends a BusPacket over a net.Conn.
func WritePacket(w io.Writer, p *BusPacket) error {
	// Fixed header:
	// Magic (4) + Type (1) + Reserved (1) + Epoch (8) + SenderID (40) + SenderIP (16) +
	// Port (2) + BusPort (2) + Flags (2) + Slots (2048) + TargetNodeID (40) + ReqEpoch (8) + GossipCount (2)
	// Total header = 2174 bytes
	var hdr [2174]byte
	copy(hdr[0:4], BusMagic[:])
	hdr[4] = p.Type
	hdr[5] = 0 // Reserved
	binary.BigEndian.PutUint64(hdr[6:14], p.Epoch)

	copy(hdr[14:54], padString(p.SenderID, 40))
	copy(hdr[54:70], padString(p.SenderIP, 16))
	binary.BigEndian.PutUint16(hdr[70:72], p.Port)
	binary.BigEndian.PutUint16(hdr[72:74], p.BusPort)
	binary.BigEndian.PutUint16(hdr[74:76], p.Flags)
	copy(hdr[76:2124], p.Slots[:])
	copy(hdr[2124:2164], padString(p.TargetNodeID, 40))
	binary.BigEndian.PutUint64(hdr[2164:2172], p.ReqEpoch)
	binary.BigEndian.PutUint16(hdr[2172:2174], uint16(len(p.Gossip)))

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}

	// Write Gossip entries
	// Each entry: NodeID (40) + IP (16) + Port (2) + BusPort (2) + Flags (2) + PingSent (8) + PongRecv (8) = 78 bytes
	if len(p.Gossip) > 0 {
		buf := make([]byte, len(p.Gossip)*78)
		for i, g := range p.Gossip {
			off := i * 78
			copy(buf[off:off+40], padString(g.NodeID, 40))
			copy(buf[off+40:off+56], padString(g.IP, 16))
			binary.BigEndian.PutUint16(buf[off+56:off+58], g.Port)
			binary.BigEndian.PutUint16(buf[off+58:off+60], g.BusPort)
			binary.BigEndian.PutUint16(buf[off+60:off+62], g.Flags)
			binary.BigEndian.PutUint64(buf[off+62:off+70], uint64(g.PingSent))
			binary.BigEndian.PutUint64(buf[off+70:off+78], uint64(g.PongRecv))
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}

	return nil
}

// ReadPacket parses an incoming BusPacket from a stream.
func ReadPacket(r io.Reader) (*BusPacket, error) {
	var hdr [2174]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}

	if hdr[0] != BusMagic[0] || hdr[1] != BusMagic[1] || hdr[2] != BusMagic[2] || hdr[3] != BusMagic[3] {
		return nil, fmt.Errorf("ERR invalid cluster bus magic bytes")
	}

	p := &BusPacket{
		Type:         hdr[4],
		Epoch:        binary.BigEndian.Uint64(hdr[6:14]),
		SenderID:     strings.TrimRight(string(hdr[14:54]), "\x00"),
		SenderIP:     strings.TrimRight(string(hdr[54:70]), "\x00"),
		Port:         binary.BigEndian.Uint16(hdr[70:72]),
		BusPort:      binary.BigEndian.Uint16(hdr[72:74]),
		Flags:        binary.BigEndian.Uint16(hdr[74:76]),
		TargetNodeID: strings.TrimRight(string(hdr[2124:2164]), "\x00"),
		ReqEpoch:     binary.BigEndian.Uint64(hdr[2164:2172]),
	}
	copy(p.Slots[:], hdr[76:2124])

	gossipCount := int(binary.BigEndian.Uint16(hdr[2172:2174]))
	if gossipCount > 0 {
		buf := make([]byte, gossipCount*78)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		p.Gossip = make([]GossipEntry, gossipCount)
		for i := 0; i < gossipCount; i++ {
			off := i * 78
			p.Gossip[i] = GossipEntry{
				NodeID:   strings.TrimRight(string(buf[off:off+40]), "\x00"),
				IP:       strings.TrimRight(string(buf[off+40:off+56]), "\x00"),
				Port:     binary.BigEndian.Uint16(buf[off+56:off+58]),
				BusPort:  binary.BigEndian.Uint16(buf[off+58:off+60]),
				Flags:    binary.BigEndian.Uint16(buf[off+60:off+62]),
				PingSent: int64(binary.BigEndian.Uint64(buf[off+62:off+70])),
				PongRecv: int64(binary.BigEndian.Uint64(buf[off+70:off+78])),
			}
		}
	}

	return p, nil
}

func padString(s string, length int) []byte {
	b := make([]byte, length)
	copy(b, []byte(s))
	return b
}
