package datastruct

import (
	"errors"
	"math"
	"sort"
	"sync"
)

var (
	ErrDimensionMismatch = errors.New("vector dimension mismatch")
	ErrVectorNotFound    = errors.New("vector not found")
)

type VectorSearchResult struct {
	ID    string   `json:"id"`
	Score float64  `json:"score"`
	Vec   []float32 `json:"vector,omitempty"`
}

type VectorIndex struct {
	mu        sync.RWMutex
	Dimension int
	Vectors   map[string][]float32
}

func NewVectorIndex(dim int) *VectorIndex {
	return &VectorIndex{
		Dimension: dim,
		Vectors:   make(map[string][]float32),
	}
}

func (vi *VectorIndex) Add(id string, vec []float32) error {
	if vi.Dimension > 0 && len(vec) != vi.Dimension {
		return ErrDimensionMismatch
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()

	if vi.Dimension == 0 {
		vi.Dimension = len(vec)
	}

	copied := make([]float32, len(vec))
	copy(copied, vec)
	vi.Vectors[id] = copied
	return nil
}

func (vi *VectorIndex) Get(id string) ([]float32, bool) {
	vi.mu.RLock()
	defer vi.mu.RUnlock()

	vec, ok := vi.Vectors[id]
	return vec, ok
}

func (vi *VectorIndex) Delete(id string) bool {
	vi.mu.Lock()
	defer vi.mu.Unlock()

	if _, exists := vi.Vectors[id]; exists {
		delete(vi.Vectors, id)
		return true
	}
	return false
}

func (vi *VectorIndex) Len() int {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	return len(vi.Vectors)
}

// Search finds Top-K nearest vectors using specified metric ("cosine" or "l2")
func (vi *VectorIndex) Search(query []float32, topK int, metric string) ([]VectorSearchResult, error) {
	vi.mu.RLock()
	defer vi.mu.RUnlock()

	if len(query) != vi.Dimension {
		return nil, ErrDimensionMismatch
	}

	if topK <= 0 {
		topK = 10
	}

	results := make([]VectorSearchResult, 0, len(vi.Vectors))

	for id, vec := range vi.Vectors {
		var score float64
		switch metric {
		case "l2", "euclidean":
			score = EuclideanDistance(query, vec)
		default: // "cosine"
			score = CosineSimilarity(query, vec)
		}
		results = append(results, VectorSearchResult{
			ID:    id,
			Score: score,
		})
	}

	if metric == "l2" || metric == "euclidean" {
		// Ascending order for distance (smaller is closer)
		sort.Slice(results, func(i, j int) bool {
			return results[i].Score < results[j].Score
		})
	} else {
		// Descending order for similarity (larger is more similar)
		sort.Slice(results, func(i, j int) bool {
			return results[i].Score > results[j].Score
		})
	}

	if len(results) > topK {
		results = results[:topK]
	}

	return results, nil
}

func CosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		valA := float64(a[i])
		valB := float64(b[i])
		dot += valA * valB
		normA += valA * valA
		normB += valB * valB
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func EuclideanDistance(a, b []float32) float64 {
	if len(a) != len(b) {
		return math.MaxFloat64
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return math.Sqrt(sum)
}
