package datastruct

import (
	"math/rand"
	"time"
)

const (
	SkipListMaxLevel = 32
	SkipListP        = 0.25
)

type ScoredMember struct {
	Member string
	Score  float64
}

type SkipListLevel struct {
	Forward *SkipListNode
	Span    uint64
}

type SkipListNode struct {
	Member   string
	Score    float64
	Backward *SkipListNode
	Level    []SkipListLevel
}

type SkipList struct {
	Header *SkipListNode
	Tail   *SkipListNode
	Length int64
	Level  int
	rng    *rand.Rand
	dict   map[string]float64 // quick member to score lookup
}

func NewSkipList() *SkipList {
	sl := &SkipList{
		Level: 1,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
		dict:  make(map[string]float64),
	}
	sl.Header = &SkipListNode{
		Level: make([]SkipListLevel, SkipListMaxLevel),
	}
	return sl
}

func (sl *SkipList) randomLevel() int {
	lvl := 1
	for float64(sl.rng.Int31()&0xFFFF) < float64(SkipListP*0xFFFF) {
		lvl++
		if lvl >= SkipListMaxLevel {
			break
		}
	}
	return lvl
}

func (sl *SkipList) Insert(score float64, member string) *SkipListNode {
	// If already present, remove old and insert new
	if _, exists := sl.dict[member]; exists {
		sl.Delete(member)
	}

	var update [SkipListMaxLevel]*SkipListNode
	var rank [SkipListMaxLevel]uint64
	x := sl.Header

	for i := sl.Level - 1; i >= 0; i-- {
		if i == sl.Level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.Level[i].Forward != nil &&
			(x.Level[i].Forward.Score < score ||
				(x.Level[i].Forward.Score == score && x.Level[i].Forward.Member < member)) {
			rank[i] += x.Level[i].Span
			x = x.Level[i].Forward
		}
		update[i] = x
	}

	level := sl.randomLevel()
	if level > sl.Level {
		for i := sl.Level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.Header
			update[i].Level[i].Span = uint64(sl.Length)
		}
		sl.Level = level
	}

	node := &SkipListNode{
		Member: member,
		Score:  score,
		Level:  make([]SkipListLevel, level),
	}

	for i := 0; i < level; i++ {
		node.Level[i].Forward = update[i].Level[i].Forward
		update[i].Level[i].Forward = node

		node.Level[i].Span = update[i].Level[i].Span - (rank[0] - rank[i])
		update[i].Level[i].Span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < sl.Level; i++ {
		update[i].Level[i].Span++
	}

	if update[0] == sl.Header {
		node.Backward = nil
	} else {
		node.Backward = update[0]
	}

	if node.Level[0].Forward != nil {
		node.Level[0].Forward.Backward = node
	} else {
		sl.Tail = node
	}

	sl.Length++
	sl.dict[member] = score
	return node
}

func (sl *SkipList) Delete(member string) bool {
	score, exists := sl.dict[member]
	if !exists {
		return false
	}

	var update [SkipListMaxLevel]*SkipListNode
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(x.Level[i].Forward.Score < score ||
				(x.Level[i].Forward.Score == score && x.Level[i].Forward.Member < member)) {
			x = x.Level[i].Forward
		}
		update[i] = x
	}

	target := x.Level[0].Forward
	if target != nil && target.Score == score && target.Member == member {
		for i := 0; i < sl.Level; i++ {
			if update[i].Level[i].Forward == target {
				update[i].Level[i].Span += target.Level[i].Span - 1
				update[i].Level[i].Forward = target.Level[i].Forward
			} else {
				update[i].Level[i].Span--
			}
		}
		if target.Level[0].Forward != nil {
			target.Level[0].Forward.Backward = target.Backward
		} else {
			sl.Tail = target.Backward
		}
		for sl.Level > 1 && sl.Header.Level[sl.Level-1].Forward == nil {
			sl.Level--
		}
		sl.Length--
		delete(sl.dict, member)
		return true
	}

	return false
}

func (sl *SkipList) GetScore(member string) (float64, bool) {
	score, ok := sl.dict[member]
	return score, ok
}

func (sl *SkipList) GetRank(member string) int64 {
	score, exists := sl.dict[member]
	if !exists {
		return -1
	}

	var rank uint64
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(x.Level[i].Forward.Score < score ||
				(x.Level[i].Forward.Score == score && x.Level[i].Forward.Member <= member)) {
			rank += x.Level[i].Span
			x = x.Level[i].Forward
		}
		if x.Member == member {
			return int64(rank) - 1 // 0-indexed rank
		}
	}
	return -1
}

func (sl *SkipList) GetElementByRank(rank uint64) *SkipListNode {
	var traversed uint64
	x := sl.Header
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && (traversed+x.Level[i].Span) <= rank {
			traversed += x.Level[i].Span
			x = x.Level[i].Forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

func (sl *SkipList) Range(start, stop int64, reverse bool) []ScoredMember {
	if sl.Length == 0 {
		return nil
	}

	// Normalize negative indexes
	if start < 0 {
		start = sl.Length + start
	}
	if stop < 0 {
		stop = sl.Length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= sl.Length {
		stop = sl.Length - 1
	}
	if start > stop || start >= sl.Length {
		return nil
	}

	results := make([]ScoredMember, 0, stop-start+1)
	if !reverse {
		node := sl.GetElementByRank(uint64(start + 1))
		for i := start; i <= stop && node != nil; i++ {
			results = append(results, ScoredMember{
				Member: node.Member,
				Score:  node.Score,
			})
			node = node.Level[0].Forward
		}
	} else {
		// Reverse range: rank starts from highest
		rank := uint64(sl.Length - start)
		node := sl.GetElementByRank(rank)
		for i := start; i <= stop && node != nil; i++ {
			results = append(results, ScoredMember{
				Member: node.Member,
				Score:  node.Score,
			})
			node = node.Backward
		}
	}

	return results
}
