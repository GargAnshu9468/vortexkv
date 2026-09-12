package datastruct

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidStreamID = errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	ErrGroupExists     = errors.New("BUSYGROUP Consumer Group name already exists")
	ErrGroupNotFound   = errors.New("NOGROUP No such consumer group for key name")
)

type StreamEntry struct {
	ID     string            `json:"id"`
	Fields map[string]string `json:"fields"`
	Order  []string          `json:"order"` // preserves field insertion order
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
	if len(parts) == 1 {
		m, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return StreamID{}, errors.New("invalid stream id format")
		}
		return StreamID{Millis: m, Seq: 0}, nil
	}
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

// CompareStreamIDs returns -1 if id1 < id2, 1 if id1 > id2, 0 if id1 == id2
func CompareStreamIDs(id1, id2 string) int {
	if id1 == id2 {
		return 0
	}
	if id1 == "-" || id2 == "+" {
		return -1
	}
	if id1 == "+" || id2 == "-" {
		return 1
	}
	sid1, err1 := ParseStreamID(id1)
	sid2, err2 := ParseStreamID(id2)
	if err1 != nil || err2 != nil {
		return strings.Compare(id1, id2)
	}
	if sid1.Millis < sid2.Millis {
		return -1
	}
	if sid1.Millis > sid2.Millis {
		return 1
	}
	if sid1.Seq < sid2.Seq {
		return -1
	}
	if sid1.Seq > sid2.Seq {
		return 1
	}
	return 0
}

// PendingEntry records a message delivered to a consumer waiting for acknowledgement
type PendingEntry struct {
	ID            string    `json:"id"`
	ConsumerName  string    `json:"consumer"`
	DeliveryTime  time.Time `json:"delivery_time"`
	DeliveryCount int64     `json:"delivery_count"`
}

type Consumer struct {
	Name     string
	SeenTime time.Time
	PEL      map[string]*PendingEntry
}

type ConsumerGroup struct {
	Name            string
	LastDeliveredID string
	Consumers       map[string]*Consumer
	PEL             map[string]*PendingEntry // ID -> PendingEntry
}

type Stream struct {
	mu      sync.RWMutex
	entries []StreamEntry
	lastID  StreamID
	groups  map[string]*ConsumerGroup
	waiters []chan struct{}
}

func NewStream() *Stream {
	return &Stream{
		entries: make([]StreamEntry, 0),
		groups:  make(map[string]*ConsumerGroup),
		waiters: make([]chan struct{}, 0),
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

	// Wake up any blocked readers
	for _, w := range s.waiters {
		close(w)
	}
	s.waiters = nil

	return assignedID, nil
}

func (s *Stream) Len() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.entries))
}

func (s *Stream) LastID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.entries) == 0 {
		return "0-0"
	}
	return s.lastID.String()
}

func (s *Stream) FirstID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.entries) == 0 {
		return "0-0"
	}
	return s.entries[0].ID
}

// Range returns entries between start and end (inclusive)
func (s *Stream) Range(start, end string, count int64) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.entries) == 0 {
		return []StreamEntry{}
	}

	var results []StreamEntry
	for _, entry := range s.entries {
		if start != "-" && start != "" && CompareStreamIDs(entry.ID, start) < 0 {
			continue
		}
		if end != "+" && end != "" && CompareStreamIDs(entry.ID, end) > 0 {
			break
		}
		results = append(results, entry)
		if count > 0 && int64(len(results)) >= count {
			break
		}
	}
	return results
}

// RevRange returns entries between end and start in reverse chronological order
func (s *Stream) RevRange(end, start string, count int64) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.entries) == 0 {
		return []StreamEntry{}
	}

	var results []StreamEntry
	for i := len(s.entries) - 1; i >= 0; i-- {
		entry := s.entries[i]
		if end != "+" && end != "" && CompareStreamIDs(entry.ID, end) > 0 {
			continue
		}
		if start != "-" && start != "" && CompareStreamIDs(entry.ID, start) < 0 {
			break
		}
		results = append(results, entry)
		if count > 0 && int64(len(results)) >= count {
			break
		}
	}
	return results
}

// Read returns entries with IDs strictly greater than lastID
func (s *Stream) Read(lastID string, count int64) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.entries) == 0 {
		return []StreamEntry{}
	}

	var results []StreamEntry
	for _, entry := range s.entries {
		if CompareStreamIDs(entry.ID, lastID) > 0 {
			results = append(results, entry)
			if count > 0 && int64(len(results)) >= count {
				break
			}
		}
	}
	return results
}

// Delete removes entries by ID and returns count removed
func (s *Stream) Delete(ids []string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	idMap := make(map[string]bool, len(ids))
	for _, id := range ids {
		idMap[id] = true
	}

	var newEntries []StreamEntry
	var removed int64
	for _, e := range s.entries {
		if idMap[e.ID] {
			removed++
		} else {
			newEntries = append(newEntries, e)
		}
	}
	s.entries = newEntries
	return removed
}

// Trim removes oldest entries so that len(entries) <= maxlen
func (s *Stream) Trim(maxlen int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	curLen := int64(len(s.entries))
	if curLen <= maxlen || maxlen < 0 {
		return 0
	}
	evicted := curLen - maxlen
	s.entries = s.entries[evicted:]
	return evicted
}

// RegisterWaiter registers a channel to be notified when a new entry is added
func (s *Stream) RegisterWaiter() (<-chan struct{}, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan struct{})
	s.waiters = append(s.waiters, ch)
	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, w := range s.waiters {
			if w == ch {
				s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
				break
			}
		}
	}
	return ch, cancel
}

// ================= Consumer Group Support =================

func (s *Stream) CreateGroup(name, startID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[name]; exists {
		return ErrGroupExists
	}

	actualStart := startID
	if startID == "$" {
		if len(s.entries) > 0 {
			actualStart = s.lastID.String()
		} else {
			actualStart = "0-0"
		}
	}

	s.groups[name] = &ConsumerGroup{
		Name:            name,
		LastDeliveredID: actualStart,
		Consumers:       make(map[string]*Consumer),
		PEL:             make(map[string]*PendingEntry),
	}
	return nil
}

func (s *Stream) DestroyGroup(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[name]; exists {
		delete(s.groups, name)
		return true
	}
	return false
}

func (s *Stream) SetGroupID(name, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[name]
	if !exists {
		return ErrGroupNotFound
	}
	if id == "$" {
		if len(s.entries) > 0 {
			group.LastDeliveredID = s.lastID.String()
		} else {
			group.LastDeliveredID = "0-0"
		}
	} else {
		group.LastDeliveredID = id
	}
	return nil
}

func (s *Stream) DeleteConsumer(groupName, consumerName string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return 0
	}
	consumer, exists := group.Consumers[consumerName]
	if !exists {
		return 0
	}
	pelCount := int64(len(consumer.PEL))
	for id := range consumer.PEL {
		delete(group.PEL, id)
	}
	delete(group.Consumers, consumerName)
	return pelCount
}

func (s *Stream) ReadGroup(groupName, consumerName string, id string, count int64, noAck bool) ([]StreamEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return nil, ErrGroupNotFound
	}

	consumer, exists := group.Consumers[consumerName]
	if !exists {
		consumer = &Consumer{
			Name:     consumerName,
			SeenTime: time.Now(),
			PEL:      make(map[string]*PendingEntry),
		}
		group.Consumers[consumerName] = consumer
	}
	consumer.SeenTime = time.Now()

	now := time.Now()
	var results []StreamEntry

	if id == ">" {
		// Read undelivered messages starting after group.LastDeliveredID
		for _, e := range s.entries {
			if CompareStreamIDs(e.ID, group.LastDeliveredID) > 0 {
				results = append(results, e)
				group.LastDeliveredID = e.ID

				if !noAck {
					pe := &PendingEntry{
						ID:            e.ID,
						ConsumerName:  consumerName,
						DeliveryTime:  now,
						DeliveryCount: 1,
					}
					group.PEL[e.ID] = pe
					consumer.PEL[e.ID] = pe
				}

				if count > 0 && int64(len(results)) >= count {
					break
				}
			}
		}
	} else {
		// Read consumer's pending messages starting at or after id
		// Find in consumer's PEL
		entryMap := make(map[string]StreamEntry, len(s.entries))
		for _, e := range s.entries {
			entryMap[e.ID] = e
		}

		var pendingIDs []string
		for pid := range consumer.PEL {
			if CompareStreamIDs(pid, id) >= 0 {
				pendingIDs = append(pendingIDs, pid)
			}
		}
		sort.Slice(pendingIDs, func(i, j int) bool {
			return CompareStreamIDs(pendingIDs[i], pendingIDs[j]) < 0
		})

		for _, pid := range pendingIDs {
			if entry, ok := entryMap[pid]; ok {
				results = append(results, entry)
				if !noAck {
					if pe, ok := consumer.PEL[pid]; ok {
						pe.DeliveryCount++
						pe.DeliveryTime = now
					}
				}
				if count > 0 && int64(len(results)) >= count {
					break
				}
			}
		}
	}

	return results, nil
}

func (s *Stream) Ack(groupName string, ids []string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return 0
	}

	var acked int64
	for _, id := range ids {
		if pe, exists := group.PEL[id]; exists {
			delete(group.PEL, id)
			if c, ok := group.Consumers[pe.ConsumerName]; ok {
				delete(c.PEL, id)
			}
			acked++
		}
	}
	return acked
}

func (s *Stream) GetPendingSummary(groupName string) (int64, string, string, map[string]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, exists := s.groups[groupName]
	if !exists {
		return 0, "", "", nil, ErrGroupNotFound
	}

	total := int64(len(group.PEL))
	if total == 0 {
		return 0, "", "", map[string]int64{}, nil
	}

	var minID, maxID string
	consumerCounts := make(map[string]int64)

	for id, pe := range group.PEL {
		if minID == "" || CompareStreamIDs(id, minID) < 0 {
			minID = id
		}
		if maxID == "" || CompareStreamIDs(id, maxID) > 0 {
			maxID = id
		}
		consumerCounts[pe.ConsumerName]++
	}

	return total, minID, maxID, consumerCounts, nil
}

func (s *Stream) GetPendingDetailed(groupName, start, end string, count int64, consumerFilter string, minIdle time.Duration) ([]PendingEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, exists := s.groups[groupName]
	if !exists {
		return nil, ErrGroupNotFound
	}

	var filtered []PendingEntry
	now := time.Now()

	for _, pe := range group.PEL {
		if consumerFilter != "" && pe.ConsumerName != consumerFilter {
			continue
		}
		if start != "-" && start != "" && CompareStreamIDs(pe.ID, start) < 0 {
			continue
		}
		if end != "+" && end != "" && CompareStreamIDs(pe.ID, end) > 0 {
			continue
		}
		if minIdle > 0 && now.Sub(pe.DeliveryTime) < minIdle {
			continue
		}
		filtered = append(filtered, *pe)
	}

	sort.Slice(filtered, func(i, j int) bool {
		return CompareStreamIDs(filtered[i].ID, filtered[j].ID) < 0
	})

	if count > 0 && int64(len(filtered)) > count {
		filtered = filtered[:count]
	}

	return filtered, nil
}

func (s *Stream) GetGroupsInfo() []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]map[string]any, 0, len(s.groups))
	for _, g := range s.groups {
		res = append(res, map[string]any{
			"name":            g.Name,
			"consumers":       len(g.Consumers),
			"pending":         len(g.PEL),
			"last-delivered-id": g.LastDeliveredID,
		})
	}
	return res
}

func (s *Stream) GetConsumersInfo(groupName string) ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, exists := s.groups[groupName]
	if !exists {
		return nil, ErrGroupNotFound
	}

	res := make([]map[string]any, 0, len(group.Consumers))
	now := time.Now()
	for _, c := range group.Consumers {
		idle := int64(now.Sub(c.SeenTime).Milliseconds())
		res = append(res, map[string]any{
			"name":    c.Name,
			"pending": len(c.PEL),
			"idle":    idle,
		})
	}
	return res, nil
}
