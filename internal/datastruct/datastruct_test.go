package datastruct

import (
	"testing"
)

func TestDataStructures(t *testing.T) {
	// 1. Test SkipList
	sl := NewSkipList()
	sl.Insert(100.0, "alice")
	sl.Insert(250.0, "charlie")
	sl.Insert(150.0, "bob")

	if sl.Length != 3 {
		t.Fatalf("Expected skiplist length 3, got %d", sl.Length)
	}

	members := sl.Range(0, -1, false)
	if len(members) != 3 || members[0].Member != "alice" || members[1].Member != "bob" || members[2].Member != "charlie" {
		t.Fatalf("Unexpected skiplist range: %+v", members)
	}

	rankBob := sl.GetRank("bob")
	if rankBob != 1 {
		t.Fatalf("Expected rank of bob to be 1, got %d", rankBob)
	}

	// 2. Test List
	l := NewList()
	l.RPush("a", "b", "c")
	l.LPush("head")
	if l.Len() != 4 {
		t.Fatalf("Expected list length 4, got %d", l.Len())
	}
	r := l.Range(0, -1)
	if len(r) != 4 || r[0] != "head" || r[3] != "c" {
		t.Fatalf("Unexpected list range: %+v", r)
	}

	// 3. Test Hash
	h := NewHash()
	h.Set("name", "vortex")
	h.IncrBy("counter", 10)
	val, ok := h.Get("name")
	if !ok || val != "vortex" {
		t.Fatalf("Hash get failed")
	}
	cVal, _ := h.Get("counter")
	if cVal != "10" {
		t.Fatalf("Hash incrby failed, got %s", cVal)
	}

	// 4. Test Vector Search
	vi := NewVectorIndex(3)
	vi.Add("doc1", []float32{1.0, 0.0, 0.0})
	vi.Add("doc2", []float32{0.0, 1.0, 0.0})
	vi.Add("doc3", []float32{0.9, 0.1, 0.0})

	results, err := vi.Search([]float32{1.0, 0.0, 0.0}, 2, "cosine")
	if err != nil {
		t.Fatalf("Vector search error: %v", err)
	}
	if len(results) != 2 || results[0].ID != "doc1" || results[1].ID != "doc3" {
		t.Fatalf("Unexpected vector search result: %+v", results)
	}

	// 5. Test Stream
	s := NewStream()
	id1, err := s.Add("*", map[string]string{"temp": "24C"}, []string{"temp"})
	if err != nil {
		t.Fatalf("Stream add error: %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("Expected stream len 1, got %d", s.Len())
	}
	entries := s.Range("-", "+", 10)
	if len(entries) != 1 || entries[0].ID != id1 {
		t.Fatalf("Unexpected stream entries: %+v", entries)
	}
}
