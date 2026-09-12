package datastruct

import (
	"strconv"
	"sync"
)

// Hash is a thread-safe field-value dictionary
type Hash struct {
	mu     sync.RWMutex
	fields map[string]string
}

func NewHash() *Hash {
	return &Hash{
		fields: make(map[string]string),
	}
}

func (h *Hash) Set(field, value string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, exists := h.fields[field]
	h.fields[field] = value
	if exists {
		return 0
	}
	return 1
}

func (h *Hash) MSet(kvs map[string]string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	var added int64
	for f, v := range kvs {
		if _, exists := h.fields[f]; !exists {
			added++
		}
		h.fields[f] = v
	}
	return added
}

func (h *Hash) Get(field string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	v, ok := h.fields[field]
	return v, ok
}

func (h *Hash) Del(fields ...string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	var count int64
	for _, f := range fields {
		if _, exists := h.fields[f]; exists {
			delete(h.fields, f)
			count++
		}
	}
	return count
}

func (h *Hash) Exists(field string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	_, ok := h.fields[field]
	return ok
}

func (h *Hash) GetAll() map[string]string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make(map[string]string, len(h.fields))
	for f, v := range h.fields {
		out[f] = v
	}
	return out
}

func (h *Hash) Keys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	keys := make([]string, 0, len(h.fields))
	for k := range h.fields {
		keys = append(keys, k)
	}
	return keys
}

func (h *Hash) Vals() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	vals := make([]string, 0, len(h.fields))
	for _, v := range h.fields {
		vals = append(vals, v)
	}
	return vals
}

func (h *Hash) Len() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return int64(len(h.fields))
}

func (h *Hash) IncrBy(field string, delta int64) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	valStr, exists := h.fields[field]
	var current int64 = 0
	if exists {
		var err error
		current, err = strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return 0, err
		}
	}
	newVal := current + delta
	h.fields[field] = strconv.FormatInt(newVal, 10)
	return newVal, nil
}
