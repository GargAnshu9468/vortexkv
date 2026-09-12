package datastruct

import (
	"container/heap"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const (
	DefaultM              = 16
	DefaultM0             = 32
	DefaultEFConstruction = 64
	DefaultEFSearch       = 64
	MaxGraphLevel         = 16
)

// DistItem represents a candidate node and its distance to a target
type DistItem struct {
	ID   string
	Dist float64
}

// MinHeap for distance items (smallest distance first)
type MinDistHeap []DistItem

func (h MinDistHeap) Len() int           { return len(h) }
func (h MinDistHeap) Less(i, j int) bool { return h[i].Dist < h[j].Dist }
func (h MinDistHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MinDistHeap) Push(x any)        { *h = append(*h, x.(DistItem)) }
func (h *MinDistHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// MaxHeap for distance items (largest distance first)
type MaxDistHeap []DistItem

func (h MaxDistHeap) Len() int           { return len(h) }
func (h MaxDistHeap) Less(i, j int) bool { return h[i].Dist > h[j].Dist }
func (h MaxDistHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MaxDistHeap) Push(x any)        { *h = append(*h, x.(DistItem)) }
func (h *MaxDistHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// HNSWNode represents an indexed vector node with multi-layer neighbors
type HNSWNode struct {
	ID        string
	Vec       []float32
	Level     int
	Neighbors [][]string // Neighbors[l] is the list of connected node IDs at layer l
}

// HNSWIndex implements Hierarchical Navigable Small World graph for vector search
type HNSWIndex struct {
	mu             sync.RWMutex
	Dimension      int
	Metric         string
	M              int
	M0             int
	EFConstruction int
	EFSearch       int
	ML             float64
	Nodes          map[string]*HNSWNode
	EntryPoint     string
	MaxLevel       int
	rnd            *rand.Rand
}

// NewHNSWIndex initializes an empty HNSW graph index
func NewHNSWIndex(dim int, metric string) *HNSWIndex {
	if metric == "" {
		metric = "cosine"
	}
	m := DefaultM
	return &HNSWIndex{
		Dimension:      dim,
		Metric:         metric,
		M:              m,
		M0:             DefaultM0,
		EFConstruction: DefaultEFConstruction,
		EFSearch:       DefaultEFSearch,
		ML:             1.0 / math.Log(float64(m)),
		Nodes:          make(map[string]*HNSWNode),
		EntryPoint:     "",
		MaxLevel:       -1,
		rnd:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Distance computes distance between two vectors according to index metric
func (h *HNSWIndex) Distance(a, b []float32) float64 {
	switch h.Metric {
	case "l2", "euclidean":
		var sum float64
		for i := range a {
			diff := float64(a[i]) - float64(b[i])
			sum += diff * diff
		}
		return math.Sqrt(sum)
	case "dot", "innerproduct":
		var dot float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
		}
		return -dot // Invert so smaller distance is higher dot product
	default: // "cosine"
		if len(a) == 0 || len(b) == 0 {
			return 1.0
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
			return 1.0
		}
		sim := dot / (math.Sqrt(normA) * math.Sqrt(normB))
		// Distance in [0, 2]
		return 1.0 - sim
	}
}

func (h *HNSWIndex) randomLevel() int {
	r := h.rnd.Float64()
	if r <= 0 {
		r = 1e-9
	}
	lvl := int(-math.Log(r) * h.ML)
	if lvl > MaxGraphLevel {
		lvl = MaxGraphLevel
	}
	return lvl
}

// Insert adds a new vector into the HNSW graph
func (h *HNSWIndex) Insert(id string, vec []float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.Dimension > 0 && len(vec) != h.Dimension {
		return ErrDimensionMismatch
	}
	if h.Dimension == 0 {
		h.Dimension = len(vec)
	}

	// If node already exists, update vector and rewire
	if old, exists := h.Nodes[id]; exists {
		old.Vec = vec
		return nil
	}

	nodeLevel := h.randomLevel()
	node := &HNSWNode{
		ID:        id,
		Vec:       vec,
		Level:     nodeLevel,
		Neighbors: make([][]string, nodeLevel+1),
	}
	for i := range node.Neighbors {
		node.Neighbors[i] = make([]string, 0, h.M0)
	}
	h.Nodes[id] = node

	// First node in graph
	if h.EntryPoint == "" {
		h.EntryPoint = id
		h.MaxLevel = nodeLevel
		return nil
	}

	curr := h.EntryPoint
	currDist := h.Distance(vec, h.Nodes[curr].Vec)

	// 1. Top-down greedy search from top layer down to nodeLevel + 1
	for l := h.MaxLevel; l > nodeLevel; l-- {
		changed := true
		for changed {
			changed = false
			cNode := h.Nodes[curr]
			if l < len(cNode.Neighbors) {
				for _, neighborID := range cNode.Neighbors[l] {
					nbr := h.Nodes[neighborID]
					if nbr == nil {
						continue
					}
					d := h.Distance(vec, nbr.Vec)
					if d < currDist {
						currDist = d
						curr = neighborID
						changed = true
					}
				}
			}
		}
	}

	// 2. Multi-layer beam search and edge linking from min(nodeLevel, MaxLevel) down to 0
	topLayer := nodeLevel
	if h.MaxLevel < topLayer {
		topLayer = h.MaxLevel
	}

	for l := topLayer; l >= 0; l-- {
		candidates := h.searchLayer(vec, curr, h.EFConstruction, l)
		m := h.M
		if l == 0 {
			m = h.M0
		}

		// Select up to m closest neighbors
		selected := h.selectNeighbors(candidates, m)
		node.Neighbors[l] = make([]string, len(selected))
		for i, it := range selected {
			node.Neighbors[l][i] = it.ID
		}

		// Add bidirectional links
		for _, it := range selected {
			nbr := h.Nodes[it.ID]
			if nbr == nil || l >= len(nbr.Neighbors) {
				continue
			}
			nbr.Neighbors[l] = append(nbr.Neighbors[l], id)
			maxConn := h.M
			if l == 0 {
				maxConn = h.M0
			}
			if len(nbr.Neighbors[l]) > maxConn {
				nbr.Neighbors[l] = h.pruneNeighbors(nbr.Vec, nbr.Neighbors[l], maxConn)
			}
		}

		if len(candidates) > 0 {
			curr = candidates[0].ID
		}
	}

	// Update entry point if new node has higher level
	if nodeLevel > h.MaxLevel {
		h.MaxLevel = nodeLevel
		h.EntryPoint = id
	}

	return nil
}

// searchLayer searches layer l with a dynamic candidate pool of size ef
func (h *HNSWIndex) searchLayer(target []float32, entryPoint string, ef int, level int) []DistItem {
	visited := make(map[string]struct{})
	vQueue := &MinDistHeap{}
	wQueue := &MaxDistHeap{}

	epNode := h.Nodes[entryPoint]
	if epNode == nil {
		return nil
	}

	d := h.Distance(target, epNode.Vec)
	heap.Init(vQueue)
	heap.Init(wQueue)

	heap.Push(vQueue, DistItem{ID: entryPoint, Dist: d})
	heap.Push(wQueue, DistItem{ID: entryPoint, Dist: d})
	visited[entryPoint] = struct{}{}

	for vQueue.Len() > 0 {
		curr := heap.Pop(vQueue).(DistItem)
		worst := (*wQueue)[0]

		if curr.Dist > worst.Dist {
			break
		}

		cNode := h.Nodes[curr.ID]
		if cNode == nil || level >= len(cNode.Neighbors) {
			continue
		}

		for _, nID := range cNode.Neighbors[level] {
			if _, seen := visited[nID]; seen {
				continue
			}
			visited[nID] = struct{}{}

			nbr := h.Nodes[nID]
			if nbr == nil {
				continue
			}

			nDist := h.Distance(target, nbr.Vec)
			worst = (*wQueue)[0]

			if nDist < worst.Dist || wQueue.Len() < ef {
				heap.Push(vQueue, DistItem{ID: nID, Dist: nDist})
				heap.Push(wQueue, DistItem{ID: nID, Dist: nDist})

				if wQueue.Len() > ef {
					heap.Pop(wQueue)
				}
			}
		}
	}

	res := make([]DistItem, wQueue.Len())
	copy(res, *wQueue)
	sort.Slice(res, func(i, j int) bool {
		return res[i].Dist < res[j].Dist
	})
	return res
}

func (h *HNSWIndex) selectNeighbors(candidates []DistItem, m int) []DistItem {
	if len(candidates) <= m {
		return candidates
	}
	return candidates[:m]
}

func (h *HNSWIndex) pruneNeighbors(base []float32, neighbors []string, m int) []string {
	type distPair struct {
		id   string
		dist float64
	}
	pairs := make([]distPair, 0, len(neighbors))
	for _, nid := range neighbors {
		node := h.Nodes[nid]
		if node == nil {
			continue
		}
		pairs = append(pairs, distPair{id: nid, dist: h.Distance(base, node.Vec)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].dist < pairs[j].dist
	})
	if len(pairs) > m {
		pairs = pairs[:m]
	}
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.id
	}
	return out
}

// Search queries the HNSW graph for topK nearest neighbors
func (h *HNSWIndex) Search(query []float32, topK int, efSearch int) ([]VectorSearchResult, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.Dimension > 0 && len(query) != h.Dimension {
		return nil, ErrDimensionMismatch
	}
	if len(h.Nodes) == 0 || h.EntryPoint == "" {
		return []VectorSearchResult{}, nil
	}

	if topK <= 0 {
		topK = 10
	}
	if efSearch < topK {
		efSearch = topK
	}

	curr := h.EntryPoint
	currDist := h.Distance(query, h.Nodes[curr].Vec)

	// 1. Greedy routing down to layer 1
	for l := h.MaxLevel; l > 0; l-- {
		changed := true
		for changed {
			changed = false
			cNode := h.Nodes[curr]
			if l < len(cNode.Neighbors) {
				for _, neighborID := range cNode.Neighbors[l] {
					nbr := h.Nodes[neighborID]
					if nbr == nil {
						continue
					}
					d := h.Distance(query, nbr.Vec)
					if d < currDist {
						currDist = d
						curr = neighborID
						changed = true
					}
				}
			}
		}
	}

	// 2. Beam search at base layer 0
	candidates := h.searchLayer(query, curr, efSearch, 0)
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}

	results := make([]VectorSearchResult, len(candidates))
	for i, c := range candidates {
		var score float64
		switch h.Metric {
		case "l2", "euclidean":
			score = c.Dist // Distance (smaller is closer)
		case "dot", "innerproduct":
			score = -c.Dist // Invert back to positive dot product
		default: // "cosine"
			score = 1.0 - c.Dist // Convert distance back to similarity in [-1, 1]
		}
		results[i] = VectorSearchResult{
			ID:    c.ID,
			Score: score,
			Vec:   h.Nodes[c.ID].Vec,
		}
	}

	return results, nil
}

// Delete removes a node from the HNSW graph and rewires its neighbors
func (h *HNSWIndex) Delete(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, exists := h.Nodes[id]
	if !exists {
		return false
	}

	// Remove references to id from all neighbors
	for l := 0; l <= node.Level; l++ {
		for _, nID := range node.Neighbors[l] {
			nbr := h.Nodes[nID]
			if nbr == nil || l >= len(nbr.Neighbors) {
				continue
			}
			newNeighbors := make([]string, 0, len(nbr.Neighbors[l]))
			for _, item := range nbr.Neighbors[l] {
				if item != id {
					newNeighbors = append(newNeighbors, item)
				}
			}
			nbr.Neighbors[l] = newNeighbors
		}
	}

	delete(h.Nodes, id)

	// If deleted node was entry point, find new entry point
	if h.EntryPoint == id {
		h.EntryPoint = ""
		h.MaxLevel = -1
		for nid, n := range h.Nodes {
			if n.Level > h.MaxLevel {
				h.MaxLevel = n.Level
				h.EntryPoint = nid
			}
		}
	}

	return true
}

// Len returns total indexed vectors
func (h *HNSWIndex) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.Nodes)
}
