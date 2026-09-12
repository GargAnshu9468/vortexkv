package datastruct

import "sync"

// Set is a thread-safe string set
type Set struct {
	mu      sync.RWMutex
	members map[string]struct{}
}

func NewSet() *Set {
	return &Set{
		members: make(map[string]struct{}),
	}
}

func (s *Set) Add(members ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var added int64
	for _, m := range members {
		if _, exists := s.members[m]; !exists {
			s.members[m] = struct{}{}
			added++
		}
	}
	return added
}

func (s *Set) Rem(members ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var removed int64
	for _, m := range members {
		if _, exists := s.members[m]; exists {
			delete(s.members, m)
			removed++
		}
	}
	return removed
}

func (s *Set) IsMember(member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.members[member]
	return exists
}

func (s *Set) Members() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.members))
	for m := range s.members {
		out = append(out, m)
	}
	return out
}

func (s *Set) Card() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.members))
}
