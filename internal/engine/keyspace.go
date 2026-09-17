package engine

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vortexkv/vortexkv/internal/datastruct"
)

const (
	NumShards = 64
)

type EntryType string

const (
	TypeString EntryType = "string"
	TypeHash   EntryType = "hash"
	TypeList   EntryType = "list"
	TypeSet    EntryType = "set"
	TypeZSet   EntryType = "zset"
	TypeVector EntryType = "vector"
	TypeStream EntryType = "stream"
)

type Entry struct {
	Type           EntryType
	Value          any
	ExpiresAt      int64 // Unix millisecond; 0 = no expiration
	UpdatedAt      int64 // Unix millisecond
	LastAccessedAt int64 // Unix millisecond for LRU tracking
}

func (e *Entry) IsExpired(nowMilli int64) bool {
	return e.ExpiresAt > 0 && e.ExpiresAt <= nowMilli
}

type Shard struct {
	mu      sync.RWMutex
	entries map[string]*Entry
	_       [64]byte // Cacheline padding to prevent false sharing across CPU cores
}

type Keyspace struct {
	shards    [NumShards]*Shard
	keyCount  atomic.Int64
	stopReaper chan struct{}
}

func NewKeyspace() *Keyspace {
	ks := &Keyspace{
		stopReaper: make(chan struct{}),
	}
	for i := 0; i < NumShards; i++ {
		ks.shards[i] = &Shard{
			entries: make(map[string]*Entry),
		}
	}

	go ks.activeExpirationReaper()
	return ks
}

// fnv1a implements ultra-fast zero-allocation 64-bit FNV-1a hash over string bytes
func fnv1a(s string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	var hash uint64 = offset64
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}
	return hash
}

func (ks *Keyspace) getShard(key string) *Shard {
	return ks.shards[fnv1a(key)%uint64(NumShards)]
}

// activeExpirationReaper implements Redis-style active probabilistic key expiration
func (ks *Keyspace) activeExpirationReaper() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ks.stopReaper:
			return
		case <-ticker.C:
			nowMilli := time.Now().UnixMilli()
			// Inspect random shards
			for s := 0; s < 16; s++ {
				shardIdx := rng.Intn(NumShards)
				shard := ks.shards[shardIdx]

				shard.mu.Lock()
				totalChecked := 0
				expiredCount := 0

				for k, entry := range shard.entries {
					totalChecked++
					if entry.IsExpired(nowMilli) {
						delete(shard.entries, k)
						ks.keyCount.Add(-1)
						expiredCount++
					}
					if totalChecked >= 20 {
						break
					}
				}
				shard.mu.Unlock()
			}
		}
	}
}

func (ks *Keyspace) Get(key string) (*Entry, bool) {
	shard := ks.getShard(key)
	shard.mu.RLock()
	entry, exists := shard.entries[key]
	shard.mu.RUnlock()

	if !exists {
		return nil, false
	}

	// Fast path: if no TTL is set (vast majority of cache keys), skip time.Now()
	if entry.ExpiresAt > 0 {
		now := time.Now().UnixMilli()
		if entry.IsExpired(now) {
			// Passive expiration on access
			shard.mu.Lock()
			if e, ok := shard.entries[key]; ok && e.IsExpired(now) {
				delete(shard.entries, key)
				ks.keyCount.Add(-1)
			}
			shard.mu.Unlock()
			return nil, false
		}
	}

	return entry, true
}

func (ks *Keyspace) Set(key string, entry *Entry) {
	shard := ks.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	now := time.Now().UnixMilli()
	entry.UpdatedAt = now
	atomic.StoreInt64(&entry.LastAccessedAt, now)
	if _, exists := shard.entries[key]; !exists {
		ks.keyCount.Add(1)
	}
	shard.entries[key] = entry
}

// EvictLRU samples keys across shards and evicts the least recently accessed keys
func (ks *Keyspace) EvictLRU(targetCount int) int64 {
	if targetCount <= 0 {
		targetCount = 10
	}

	type Candidate struct {
		Key          string
		LastAccessed int64
		ShardIdx     int
	}

	var candidates []Candidate

	// Sample candidate keys from multiple shards
	for i := 0; i < NumShards && len(candidates) < targetCount*3; i++ {
		shard := ks.shards[i]
		shard.mu.RLock()
		sampled := 0
		for k, entry := range shard.entries {
			candidates = append(candidates, Candidate{
				Key:          k,
				LastAccessed: atomic.LoadInt64(&entry.LastAccessedAt),
				ShardIdx:     i,
			})
			sampled++
			if sampled >= 10 {
				break
			}
		}
		shard.mu.RUnlock()
	}

	if len(candidates) == 0 {
		return 0
	}

	// Sort candidates ascending by LastAccessed (oldest first)
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].LastAccessed < candidates[i].LastAccessed {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	var evicted int64
	for _, c := range candidates {
		shard := ks.shards[c.ShardIdx]
		shard.mu.Lock()
		if _, exists := shard.entries[c.Key]; exists {
			delete(shard.entries, c.Key)
			ks.keyCount.Add(-1)
			evicted++
		}
		shard.mu.Unlock()

		if evicted >= int64(targetCount) {
			break
		}
	}

	return evicted
}

func (ks *Keyspace) Delete(keys ...string) int64 {
	var count int64
	for _, key := range keys {
		shard := ks.getShard(key)
		shard.mu.Lock()
		if _, exists := shard.entries[key]; exists {
			delete(shard.entries, key)
			ks.keyCount.Add(-1)
			count++
		}
		shard.mu.Unlock()
	}
	return count
}

func (ks *Keyspace) Exists(keys ...string) int64 {
	var count int64
	for _, key := range keys {
		if _, ok := ks.Get(key); ok {
			count++
		}
	}
	return count
}

func (ks *Keyspace) Expire(key string, ttlMillis int64) bool {
	entry, ok := ks.Get(key)
	if !ok {
		return false
	}

	shard := ks.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry.ExpiresAt = time.Now().UnixMilli() + ttlMillis
	return true
}

func (ks *Keyspace) TTL(key string) int64 {
	entry, ok := ks.Get(key)
	if !ok {
		return -2 // key does not exist
	}
	if entry.ExpiresAt == 0 {
		return -1 // key exists but has no associated expire
	}
	remaining := entry.ExpiresAt - time.Now().UnixMilli()
	if remaining <= 0 {
		return -2
	}
	return remaining / 1000
}

func (ks *Keyspace) PTTL(key string) int64 {
	entry, ok := ks.Get(key)
	if !ok {
		return -2
	}
	if entry.ExpiresAt == 0 {
		return -1
	}
	remaining := entry.ExpiresAt - time.Now().UnixMilli()
	if remaining <= 0 {
		return -2
	}
	return remaining
}

func (ks *Keyspace) Persist(key string) bool {
	entry, ok := ks.Get(key)
	if !ok || entry.ExpiresAt == 0 {
		return false
	}
	shard := ks.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry.ExpiresAt = 0
	return true
}

func (ks *Keyspace) TotalKeys() int64 {
	return ks.keyCount.Load()
}

func (ks *Keyspace) Keys(pattern string) []string {
	now := time.Now().UnixMilli()
	var keys []string

	matchAll := pattern == "*" || pattern == ""

	for i := 0; i < NumShards; i++ {
		shard := ks.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if entry.IsExpired(now) {
				continue
			}
			if matchAll {
				keys = append(keys, k)
				continue
			}
			matched, err := filepath.Match(pattern, k)
			if err == nil && matched {
				keys = append(keys, k)
			} else if strings.Contains(pattern, "*") {
				// simple fallback prefix match
				prefix := strings.TrimSuffix(pattern, "*")
				if strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
				}
			}
		}
		shard.mu.RUnlock()
	}

	return keys
}

type KeyMeta struct {
	Key       string    `json:"key"`
	Type      EntryType `json:"type"`
	TTL       int64     `json:"ttl"`
	Size      int       `json:"size"`
	UpdatedAt int64     `json:"updated_at"`
}

func (ks *Keyspace) ListKeyMetas(pattern string, limit int) []KeyMeta {
	now := time.Now().UnixMilli()
	var metas []KeyMeta

	matchAll := pattern == "*" || pattern == ""

	for i := 0; i < NumShards; i++ {
		shard := ks.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if entry.IsExpired(now) {
				continue
			}
			if !matchAll {
				matched, _ := filepath.Match(pattern, k)
				if !matched {
					continue
				}
			}

			var ttl int64 = -1
			if entry.ExpiresAt > 0 {
				ttl = (entry.ExpiresAt - now) / 1000
				if ttl < 0 {
					continue
				}
			}

			size := 1
			switch entry.Type {
			case TypeString:
				if str, ok := entry.Value.(string); ok {
					size = len(str)
				}
			case TypeHash:
				if h, ok := entry.Value.(*datastruct.Hash); ok {
					size = int(h.Len())
				}
			case TypeList:
				if l, ok := entry.Value.(*datastruct.List); ok {
					size = int(l.Len())
				}
			case TypeSet:
				if s, ok := entry.Value.(*datastruct.Set); ok {
					size = int(s.Card())
				}
			case TypeZSet:
				if z, ok := entry.Value.(*datastruct.SkipList); ok {
					size = int(z.Length)
				}
			case TypeVector:
				if v, ok := entry.Value.(*datastruct.VectorIndex); ok {
					size = len(v.Vectors)
				}
			case TypeStream:
				if st, ok := entry.Value.(*datastruct.Stream); ok {
					size = int(st.Len())
				}
			}

			metas = append(metas, KeyMeta{
				Key:       k,
				Type:      entry.Type,
				TTL:       ttl,
				Size:      size,
				UpdatedAt: entry.UpdatedAt,
			})

			if limit > 0 && len(metas) >= limit {
				shard.mu.RUnlock()
				return metas
			}
		}
		shard.mu.RUnlock()
	}

	return metas
}

type StreamMeta struct {
	Key            string `json:"key"`
	Length         int64  `json:"length"`
	FirstEntryID   string `json:"first_entry_id"`
	LastEntryID    string `json:"last_entry_id"`
	ConsumerGroups int    `json:"consumer_groups"`
	UpdatedAt      int64  `json:"updated_at"`
}

func (ks *Keyspace) ListStreams() []StreamMeta {
	now := time.Now().UnixMilli()
	var streams []StreamMeta

	for i := 0; i < NumShards; i++ {
		shard := ks.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if entry.IsExpired(now) || entry.Type != TypeStream {
				continue
			}
			st, ok := entry.Value.(*datastruct.Stream)
			if !ok {
				continue
			}
			streams = append(streams, StreamMeta{
				Key:            k,
				Length:         st.Len(),
				FirstEntryID:   st.FirstID(),
				LastEntryID:    st.LastID(),
				ConsumerGroups: len(st.GetGroupsInfo()),
				UpdatedAt:      entry.UpdatedAt,
			})
		}
		shard.mu.RUnlock()
	}

	return streams
}

func (ks *Keyspace) FlushDB() {
	for i := 0; i < NumShards; i++ {
		shard := ks.shards[i]
		shard.mu.Lock()
		shard.entries = make(map[string]*Entry)
		shard.mu.Unlock()
	}
	ks.keyCount.Store(0)
}

// DumpAllCommands serializes all active keyspace entries into standard write commands
// for full resynchronization of connecting replicas.
func (ks *Keyspace) DumpAllCommands() [][]string {
	var cmds [][]string
	now := time.Now().UnixMilli()

	for i := 0; i < NumShards; i++ {
		shard := ks.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.entries {
			if entry.IsExpired(now) {
				continue
			}

			switch entry.Type {
			case TypeString:
				if s, ok := entry.Value.(string); ok {
					cmds = append(cmds, []string{"SET", k, s})
				}
			case TypeHash:
				if h, ok := entry.Value.(*datastruct.Hash); ok {
					fields := h.GetAll()
					if len(fields) > 0 {
						hsetArgs := []string{"HSET", k}
						for f, v := range fields {
							hsetArgs = append(hsetArgs, f, v)
						}
						cmds = append(cmds, hsetArgs)
					}
				}
			case TypeList:
				if l, ok := entry.Value.(*datastruct.List); ok {
					items := l.Range(0, -1)
					if len(items) > 0 {
						rpushArgs := append([]string{"RPUSH", k}, items...)
						cmds = append(cmds, rpushArgs)
					}
				}
			case TypeSet:
				if s, ok := entry.Value.(*datastruct.Set); ok {
					members := s.Members()
					if len(members) > 0 {
						saddArgs := append([]string{"SADD", k}, members...)
						cmds = append(cmds, saddArgs)
					}
				}
			case TypeZSet:
				if z, ok := entry.Value.(*datastruct.SkipList); ok {
					items := z.Range(0, -1, false)
					if len(items) > 0 {
						zaddArgs := []string{"ZADD", k}
						for _, it := range items {
							zaddArgs = append(zaddArgs, strconv.FormatFloat(it.Score, 'f', -1, 64), it.Member)
						}
						cmds = append(cmds, zaddArgs)
					}
				}
			case TypeVector:
				if v, ok := entry.Value.(*datastruct.VectorIndex); ok {
					for id, vec := range v.GetAll() {
						vaddArgs := []string{"VADD", k, id}
						for _, f := range vec {
							vaddArgs = append(vaddArgs, fmt.Sprintf("%f", f))
						}
						cmds = append(cmds, vaddArgs)
					}
				}
			case TypeStream:
				if s, ok := entry.Value.(*datastruct.Stream); ok {
					entries := s.Range("-", "+", 0)
					for _, item := range entries {
						xaddArgs := []string{"XADD", k, item.ID}
						for f, v := range item.Fields {
							xaddArgs = append(xaddArgs, f, v)
						}
						cmds = append(cmds, xaddArgs)
					}
				}
			}

			// If key has TTL, preserve expiration
			if entry.ExpiresAt > 0 {
				cmds = append(cmds, []string{"PEXPIREAT", k, strconv.FormatInt(entry.ExpiresAt, 10)})
			}
		}
		shard.mu.RUnlock()
	}

	return cmds
}

func (ks *Keyspace) Close() {
	close(ks.stopReaper)
}

