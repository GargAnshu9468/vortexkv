package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ClusterNode represents a physical node in the distributed VortexKV cluster.
type ClusterNode struct {
	ID        string         `json:"id"`
	IP        string         `json:"ip"`
	Port      int            `json:"port"`
	BusPort   int            `json:"bus_port"`
	Role      string         `json:"role"` // "master" or "slave"
	MasterID  string         `json:"master_id"`
	Slots     [16384]bool    `json:"slots"`
	Epoch     uint64         `json:"epoch"`
	LinkState string         `json:"link_state"` // "connected" or "disconnected"
	PingSent  int64          `json:"ping_sent"`
	PongRecv  int64          `json:"pong_recv"`
	PFail     bool           `json:"pfail"`
	Fail      bool           `json:"fail"`
}

// GenerateNodeID creates a cryptographic 40-character hex node identifier.
func GenerateNodeID() string {
	b := make([]byte, 20)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// NewNode instantiates a new cluster node descriptor.
func NewNode(id, ip string, port, busPort int, role string) *ClusterNode {
	if id == "" {
		id = GenerateNodeID()
	}
	if ip == "" {
		ip = "127.0.0.1"
	}
	if busPort <= 0 {
		busPort = port + 10000
	}
	if role == "" {
		role = "master"
	}
	return &ClusterNode{
		ID:        id,
		IP:        ip,
		Port:      port,
		BusPort:   busPort,
		Role:      role,
		MasterID:  "-",
		Epoch:     1,
		LinkState: "connected",
		PingSent:  0,
		PongRecv:  time.Now().UnixMilli(),
		PFail:     false,
		Fail:      false,
	}
}

// SlotCount returns the total number of hash slots owned by this node.
func (n *ClusterNode) SlotCount() int {
	cnt := 0
	for _, s := range n.Slots {
		if s {
			cnt++
		}
	}
	return cnt
}

// SlotsList returns an ordered slice of all slot indices owned by this node.
func (n *ClusterNode) SlotsList() []uint16 {
	res := make([]uint16, 0, 16384)
	for i, s := range n.Slots {
		if s {
			res = append(res, uint16(i))
		}
	}
	return res
}

// SlotRangesString formats owned slots as compact ranges (e.g., "0-5460 10923-16383").
func (n *ClusterNode) SlotRangesString() string {
	slots := n.SlotsList()
	if len(slots) == 0 {
		return ""
	}

	var ranges []string
	start := slots[0]
	prev := slots[0]

	for i := 1; i < len(slots); i++ {
		curr := slots[i]
		if curr == prev+1 {
			prev = curr
		} else {
			if start == prev {
				ranges = append(ranges, fmt.Sprintf("%d", start))
			} else {
				ranges = append(ranges, fmt.Sprintf("%d-%d", start, prev))
			}
			start = curr
			prev = curr
		}
	}
	if start == prev {
		ranges = append(ranges, fmt.Sprintf("%d", start))
	} else {
		ranges = append(ranges, fmt.Sprintf("%d-%d", start, prev))
	}

	return strings.Join(ranges, " ")
}

// FormatNodeLine serializes the node into a standard Redis CLUSTER NODES line.
// Format: <id> <ip:port@cport> <flags> <master-id> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots...>
func (n *ClusterNode) FormatNodeLine(isMyself bool) string {
	var flags []string
	if isMyself {
		flags = append(flags, "myself")
	}
	flags = append(flags, n.Role)
	if n.Fail {
		flags = append(flags, "fail")
	} else if n.PFail {
		flags = append(flags, "fail?")
	}

	masterID := n.MasterID
	if masterID == "" {
		masterID = "-"
	}

	slotStr := ""
	if n.Role == "master" {
		slotStr = n.SlotRangesString()
	}

	line := fmt.Sprintf("%s %s:%d@%d %s %s %d %d %d %s",
		n.ID,
		n.IP, n.Port, n.BusPort,
		strings.Join(flags, ","),
		masterID,
		n.PingSent,
		n.PongRecv,
		n.Epoch,
		n.LinkState,
	)

	if slotStr != "" {
		line += " " + slotStr
	}

	return line
}

// SlotRange defines a contiguous range of hash slots [Start, End].
type SlotRange struct {
	Start uint16
	End   uint16
}

// ParseSlotRanges parses space-separated slot specifications like "0-5460 7000 8000-9000".
func ParseSlotRanges(spec string) ([]uint16, error) {
	parts := strings.Fields(spec)
	var slots []uint16
	for _, p := range parts {
		if strings.Contains(p, "-") {
			var start, end uint16
			_, err := fmt.Sscanf(p, "%d-%d", &start, &end)
			if err != nil || start > end || end >= 16384 {
				return nil, fmt.Errorf("invalid slot range: %s", p)
			}
			for s := start; s <= end; s++ {
				slots = append(slots, s)
			}
		} else {
			var s uint16
			_, err := fmt.Sscanf(p, "%d", &s)
			if err != nil || s >= 16384 {
				return nil, fmt.Errorf("invalid slot: %s", p)
			}
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	return slots, nil
}
