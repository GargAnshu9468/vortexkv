package datastruct

import (
	"sync"
)

// List is a thread-safe double-ended deque implemented over a slice with head/tail pointers
type List struct {
	mu    sync.RWMutex
	items []string
}

func NewList() *List {
	return &List{
		items: make([]string, 0),
	}
}

func (l *List) LPush(values ...string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	// In Redis, LPUSH foo a b pushes 'a' then 'b', so the last one in the argument list becomes the new head
	newItems := make([]string, len(values)+len(l.items))
	for i, val := range values {
		newItems[len(values)-1-i] = val
	}
	copy(newItems[len(values):], l.items)
	l.items = newItems
	return int64(len(l.items))
}

func (l *List) RPush(values ...string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.items = append(l.items, values...)
	return int64(len(l.items))
}

func (l *List) LPop(count int) ([]string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.items) == 0 {
		return nil, false
	}
	if count > len(l.items) {
		count = len(l.items)
	}

	popped := make([]string, count)
	copy(popped, l.items[:count])
	l.items = l.items[count:]
	return popped, true
}

func (l *List) RPop(count int) ([]string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := len(l.items)
	if n == 0 {
		return nil, false
	}
	if count > n {
		count = n
	}

	start := n - count
	popped := make([]string, count)
	// Return in order of pop from right
	for i := 0; i < count; i++ {
		popped[i] = l.items[n-1-i]
	}
	l.items = l.items[:start]
	return popped, true
}

func (l *List) Len() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int64(len(l.items))
}

func (l *List) Range(start, stop int64) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	n := int64(len(l.items))
	if n == 0 {
		return []string{}
	}

	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return []string{}
	}

	result := make([]string, stop-start+1)
	copy(result, l.items[start:stop+1])
	return result
}

func (l *List) Index(index int64) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	n := int64(len(l.items))
	if index < 0 {
		index = n + index
	}
	if index < 0 || index >= n {
		return "", false
	}
	return l.items[index], true
}
