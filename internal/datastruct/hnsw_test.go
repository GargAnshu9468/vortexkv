package datastruct

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestHNSWBasicInsertAndSearch(t *testing.T) {
	vi := NewVectorIndex(4)

	vec1 := []float32{1.0, 0.0, 0.0, 0.0}
	vec2 := []float32{0.0, 1.0, 0.0, 0.0}
	vec3 := []float32{0.9, 0.1, 0.0, 0.0}
	vec4 := []float32{0.0, 0.0, 1.0, 0.0}

	if err := vi.Add("v1", vec1); err != nil {
		t.Fatalf("Failed to add v1: %v", err)
	}
	if err := vi.Add("v2", vec2); err != nil {
		t.Fatalf("Failed to add v2: %v", err)
	}
	if err := vi.Add("v3", vec3); err != nil {
		t.Fatalf("Failed to add v3: %v", err)
	}
	if err := vi.Add("v4", vec4); err != nil {
		t.Fatalf("Failed to add v4: %v", err)
	}

	if vi.Len() != 4 {
		t.Fatalf("Expected 4 items, got %d", vi.Len())
	}

	// Search for vector closest to [1.0, 0.05, 0.0, 0.0]
	query := []float32{1.0, 0.05, 0.0, 0.0}
	res, err := vi.Search(query, 2, "cosine", 64)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(res))
	}
	if res[0].ID != "v1" && res[0].ID != "v3" {
		t.Fatalf("Expected v1 or v3 as top result, got %s", res[0].ID)
	}

	info := vi.Info()
	if info["dimension"] != 4 || info["count"] != 4 {
		t.Fatalf("Unexpected info: %v", info)
	}
}

func TestHNSWRecallVsExact(t *testing.T) {
	dim := 16
	numVectors := 500
	topK := 10
	vi := NewVectorIndex(dim)

	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			vec[d] = rnd.Float32()
		}
		if err := vi.Add(fmt.Sprintf("item_%d", i), vec); err != nil {
			t.Fatalf("Failed to add vector: %v", err)
		}
	}

	// Generate 10 test queries and calculate average recall@10
	var totalRecall float64
	numQueries := 10

	for q := 0; q < numQueries; q++ {
		query := make([]float32, dim)
		for d := 0; d < dim; d++ {
			query[d] = rnd.Float32()
		}

		exactRes, err := vi.exactSearch(query, topK, "cosine")
		if err != nil {
			t.Fatalf("Exact search failed: %v", err)
		}

		hnswRes, err := vi.Search(query, topK, "cosine", 64)
		if err != nil {
			t.Fatalf("HNSW search failed: %v", err)
		}

		exactSet := make(map[string]struct{})
		for _, r := range exactRes {
			exactSet[r.ID] = struct{}{}
		}

		var matches int
		for _, r := range hnswRes {
			if _, ok := exactSet[r.ID]; ok {
				matches++
			}
		}

		recall := float64(matches) / float64(topK)
		totalRecall += recall
	}

	avgRecall := totalRecall / float64(numQueries)
	t.Logf("Average recall@%d across %d vectors: %.2f%%", topK, numVectors, avgRecall*100)

	// HNSW should have > 90% recall
	if avgRecall < 0.90 {
		t.Fatalf("Recall too low: %.2f%% (expected >= 90%%)", avgRecall*100)
	}
}

func TestHNSWDeleteAndRewire(t *testing.T) {
	vi := NewVectorIndex(3)

	_ = vi.Add("a", []float32{1, 0, 0})
	_ = vi.Add("b", []float32{0, 1, 0})
	_ = vi.Add("c", []float32{0, 0, 1})

	if vi.Len() != 3 {
		t.Fatalf("Expected 3 items, got %d", vi.Len())
	}

	deleted := vi.Delete("b")
	if !deleted {
		t.Fatalf("Expected b to be deleted")
	}
	if vi.Len() != 2 {
		t.Fatalf("Expected 2 items after delete, got %d", vi.Len())
	}

	// Search closest to [0, 1, 0] should now return a or c, not b
	res, err := vi.Search([]float32{0, 1, 0}, 2, "cosine", 64)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	for _, r := range res {
		if r.ID == "b" {
			t.Fatalf("Deleted vector 'b' was returned in search results!")
		}
	}
}

func TestHNSWMetrics(t *testing.T) {
	vi := NewVectorIndex(2)

	_ = vi.Add("origin", []float32{0, 0})
	_ = vi.Add("p1", []float32{1, 1})
	_ = vi.Add("p2", []float32{3, 4})

	// Euclidean distance from [0,0]: p1 is sqrt(2)=1.414, p2 is sqrt(25)=5
	l2Res, err := vi.Search([]float32{0, 0}, 2, "euclidean", 32)
	if err != nil {
		t.Fatalf("L2 search failed: %v", err)
	}
	if len(l2Res) < 2 || l2Res[0].ID != "origin" || l2Res[1].ID != "p1" {
		t.Fatalf("Unexpected L2 search order: %v", l2Res)
	}

	// Dot product with [1, 1]: p2 dot [1,1] = 7, p1 dot [1,1] = 2
	dotRes, err := vi.Search([]float32{1, 1}, 2, "dot", 32)
	if err != nil {
		t.Fatalf("Dot search failed: %v", err)
	}
	if len(dotRes) < 2 || dotRes[0].ID != "p2" || dotRes[1].ID != "p1" {
		t.Fatalf("Unexpected Dot product search order: %v", dotRes)
	}
}
