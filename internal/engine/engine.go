package engine

import (
	"crypto/subtle"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vortexkv/vortexkv/internal/datastruct"
	"github.com/vortexkv/vortexkv/internal/persistence"
	"github.com/vortexkv/vortexkv/internal/pubsub"
	"github.com/vortexkv/vortexkv/internal/replication"
	"github.com/vortexkv/vortexkv/internal/resp"
	"github.com/vortexkv/vortexkv/internal/telemetry"
)

type ClientSession struct {
	Authenticated bool
	Username      string
	InMulti       bool
	TxQueue       [][]string
}

type Engine struct {
	Keyspace       *Keyspace
	Broker         *pubsub.Broker
	Telemetry      *telemetry.Telemetry
	AOF            *persistence.AOF
	ACL            *ACLManager
	Replication    *replication.ReplicationManager
	Password       string
	MaxMemory      uint64 // in bytes; 0 = unlimited
	EvictionPolicy string // "allkeys-lru", "volatile-lru", "noeviction"

	muClients      sync.RWMutex
	clientSessions map[string]*ClientSession
}

func NewEngine(aofPath string, fsyncPolicy persistence.FsyncPolicy) (*Engine, error) {
	ks := NewKeyspace()
	broker := pubsub.NewBroker()
	tele := telemetry.NewTelemetry()
	acl := NewACLManager("")

	var aof *persistence.AOF
	if aofPath != "" {
		var err error
		aof, err = persistence.OpenAOF(aofPath, fsyncPolicy)
		if err != nil {
			return nil, err
		}

		// Replay existing AOF
		eTemp := &Engine{
			Keyspace:       ks,
			Broker:         broker,
			Telemetry:      tele,
			ACL:            acl,
			clientSessions: make(map[string]*ClientSession),
		}
		_ = persistence.Replay(aofPath, func(args []string) error {
			eTemp.ExecuteCommand("aof_replay", args)
			return nil
		})
	}

	eng := &Engine{
		Keyspace:       ks,
		Broker:         broker,
		Telemetry:      tele,
		AOF:            aof,
		ACL:            acl,
		EvictionPolicy: "allkeys-lru",
		clientSessions: make(map[string]*ClientSession),
	}
	eng.Replication = replication.NewReplicationManager(
		7379,
		func(args []string) {
			eng.ExecuteCommand("replica_stream", args)
		},
		func() [][]string {
			return eng.Keyspace.DumpAllCommands()
		},
	)
	return eng, nil
}

func (e *Engine) SetMasterPassword(pass string) {
	e.Password = pass
	if e.ACL != nil {
		if def, ok := e.ACL.GetUser("default"); ok {
			def.Password = pass
			e.ACL.SetUser(def)
		}
	}
}

func (e *Engine) GetClientSession(connID string) *ClientSession {
	e.muClients.Lock()
	defer e.muClients.Unlock()

	session, exists := e.clientSessions[connID]
	if !exists {
		session = &ClientSession{
			Authenticated: e.Password == "" || connID == "replica_stream" || connID == "aof_replay" || connID == "", // internal replay & replication are auto-authenticated
			Username:      "default",
		}
		e.clientSessions[connID] = session
	}
	return session
}

func (e *Engine) ClearClientSession(connID string) {
	e.muClients.Lock()
	delete(e.clientSessions, connID)
	e.muClients.Unlock()
}

func (e *Engine) Close() error {
	e.Keyspace.Close()
	if e.AOF != nil {
		return e.AOF.Close()
	}
	return nil
}

// ExecuteCommand executes a command array and tracks metrics & AOF persistence
func (e *Engine) ExecuteCommand(connID string, args []string) resp.Value {
	if len(args) == 0 {
		return resp.Error("ERR empty command")
	}

	start := time.Now()
	cmdName := strings.ToUpper(args[0])

	session := e.GetClientSession(connID)

	// Authentication check
	if e.Password != "" && !session.Authenticated && connID != "replica_stream" && connID != "aof_replay" && connID != "" {
		if cmdName != "AUTH" && cmdName != "QUIT" {
			return resp.Error("NOAUTH Authentication required.")
		}
	}

	// ACL Permission check
	if session.Authenticated && e.ACL != nil && cmdName != "AUTH" && cmdName != "QUIT" && connID != "replica_stream" && connID != "aof_replay" && connID != "" {
		keys := extractCommandKeys(cmdName, args[1:])
		if ok, reason := e.ACL.CanExecute(session.Username, cmdName, keys); !ok {
			return resp.Error(fmt.Sprintf("NOPERM %s", reason))
		}
	}

	// Read-only replica guard: Reject mutating commands if in replica mode
	if e.Replication != nil && e.Replication.ReadOnly && connID != "replica_stream" && connID != "aof_replay" {
		if isWriteCommand(cmdName) {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
	}

	// Transaction buffering if client is inside MULTI
	if session.InMulti {
		if cmdName == "EXEC" {
			return e.executeTransaction(connID, session)
		} else if cmdName == "DISCARD" {
			session.InMulti = false
			session.TxQueue = nil
			return resp.SimpleString("OK")
		} else if cmdName == "MULTI" {
			return resp.Error("ERR MULTI calls can not be nested")
		} else {
			session.TxQueue = append(session.TxQueue, args)
			return resp.SimpleString("QUEUED")
		}
	}

	val, isWrite := e.dispatch(connID, cmdName, args[1:])

	// MaxMemory enforcement for mutating commands
	if isWrite && e.MaxMemory > 0 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.Alloc > e.MaxMemory {
			if e.EvictionPolicy == "allkeys-lru" {
				for iter := 0; iter < 5 && m.Alloc > e.MaxMemory; iter++ {
					evicted := e.Keyspace.EvictLRU(50)
					if evicted == 0 {
						break
					}
					runtime.GC()
					runtime.ReadMemStats(&m)
				}
			}
			if m.Alloc > e.MaxMemory {
				return resp.Error("OOM command not allowed when used memory > 'maxmemory'")
			}
		}
	}

	durationMicro := time.Since(start).Microseconds()
	e.Telemetry.RecordCommand(cmdName, durationMicro, args)

	// Persist mutating commands to AOF
	if isWrite && e.AOF != nil {
		_ = e.AOF.WriteCommand(args)
	}

	// Broadcast mutating commands to active replicas
	if isWrite && e.Replication != nil && connID != "replica_stream" {
		e.Replication.Broadcast(args)
	}

	return val
}

func (e *Engine) executeTransaction(connID string, session *ClientSession) resp.Value {
	queue := session.TxQueue
	session.InMulti = false
	session.TxQueue = nil

	results := make([]resp.Value, len(queue))
	for i, cmdArgs := range queue {
		if len(cmdArgs) == 0 {
			continue
		}
		cName := strings.ToUpper(cmdArgs[0])
		val, isWrite := e.dispatch(connID, cName, cmdArgs[1:])
		results[i] = val
		if isWrite && e.AOF != nil {
			_ = e.AOF.WriteCommand(cmdArgs)
		}
		if isWrite && e.Replication != nil && connID != "replica_stream" {
			e.Replication.Broadcast(cmdArgs)
		}
	}
	return resp.Array(results)
}

func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "MSET", "DEL", "EXPIRE", "PEXPIREAT", "EXPIREAT", "PERSIST",
		"INCR", "INCRBY", "DECR", "DECRBY", "APPEND",
		"HSET", "HMSET", "HDEL", "HINCRBY",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LTRIM",
		"SADD", "SREM", "SPOP",
		"ZADD", "ZREM", "ZINCRBY", "ZREMRANGEBYSCORE",
		"VADD", "FLUSHDB", "FLUSHALL", "XADD", "XDEL", "XTRIM", "XGROUP", "XACK":
		return true
	default:
		return false
	}
}

func (e *Engine) dispatch(connID string, cmd string, args []string) (resp.Value, bool) {
	switch cmd {
	// ================= Server & Connection Commands =================
	case "AUTH":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'auth' command"), false
		}
		username := "default"
		password := args[0]
		if len(args) >= 2 {
			username = args[0]
			password = args[1]
		}
		if username == "default" && e.ACL != nil {
			if def, ok := e.ACL.GetUser("default"); ok && def.Password != e.Password {
				def.Password = e.Password
				e.ACL.SetUser(def)
			}
		}
		if e.ACL != nil {
			user, ok := e.ACL.Authenticate(username, password)
			if ok {
				session := e.GetClientSession(connID)
				session.Authenticated = true
				session.Username = user.Username
				return resp.SimpleString("OK"), false
			}
			return resp.Error("ERR invalid password"), false
		}
		if e.Password == "" || subtle.ConstantTimeCompare([]byte(password), []byte(e.Password)) == 1 {
			session := e.GetClientSession(connID)
			session.Authenticated = true
			session.Username = username
			return resp.SimpleString("OK"), false
		}
		return resp.Error("ERR invalid password"), false

	case "ACL":
		return e.handleACLCommand(connID, args)

	case "MULTI":
		session := e.GetClientSession(connID)
		session.InMulti = true
		session.TxQueue = nil
		return resp.SimpleString("OK"), false

	case "EXEC":
		return resp.Error("ERR EXEC without MULTI"), false

	case "DISCARD":
		return resp.Error("ERR DISCARD without MULTI"), false

	case "BGREWRITEAOF":
		return resp.SimpleString("Background append only file rewriting started"), false

	case "PING":
		if len(args) == 0 {
			return resp.SimpleString("PONG"), false
		}
		return resp.BulkString(args[0]), false

	case "ECHO":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'echo' command"), false
		}
		return resp.BulkString(args[0]), false

	case "COMMAND":
		return resp.Array([]resp.Value{}), false

	case "CLIENT":
		if len(args) > 0 && strings.ToUpper(args[0]) == "SETNAME" {
			return resp.SimpleString("OK"), false
		}
		return resp.SimpleString("OK"), false

	case "SELECT":
		return resp.SimpleString("OK"), false

	case "REPLICAOF", "SLAVEOF":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'replicaof' command"), false
		}
		if strings.ToUpper(args[0]) == "NO" && strings.ToUpper(args[1]) == "ONE" {
			if e.Replication != nil {
				e.Replication.PromoteToMaster()
			}
			return resp.SimpleString("OK"), false
		}
		port, err := strconv.Atoi(args[1])
		if err != nil {
			return resp.Error("ERR invalid port"), false
		}
		if e.Replication != nil {
			e.Replication.ConnectToMaster(args[0], port, e.Password)
		}
		return resp.SimpleString("OK"), false

	case "REPLCONF":
		return resp.SimpleString("OK"), false

	case "ROLE":
		if e.Replication == nil || e.Replication.Role == replication.RoleMaster {
			offset := int64(0)
			if e.Replication != nil {
				offset = e.Replication.MasterOffset.Load()
			}
			var slaves []resp.Value
			return resp.Array([]resp.Value{
				resp.BulkString("master"),
				resp.Integer(offset),
				resp.Array(slaves),
			}), false
		}
		linkStatus := "up"
		return resp.Array([]resp.Value{
			resp.BulkString("slave"),
			resp.BulkString(e.Replication.MasterHost),
			resp.Integer(int64(e.Replication.MasterPort)),
			resp.BulkString(linkStatus),
			resp.Integer(e.Replication.MasterOffset.Load()),
		}), false

	case "INFO":
		infoText := e.Telemetry.GenerateRedisInfo(e.Keyspace.TotalKeys())
		if e.Replication != nil {
			if len(args) > 0 && strings.ToLower(args[0]) == "replication" {
				return resp.BulkString(e.Replication.GenerateReplicationInfo()), false
			}
			infoText += "\r\n" + e.Replication.GenerateReplicationInfo()
		}
		return resp.BulkString(infoText), false

	case "DBSIZE":
		return resp.Integer(e.Keyspace.TotalKeys()), false

	case "FLUSHDB", "FLUSHALL":
		e.Keyspace.FlushDB()
		e.Telemetry.BroadcastEvent(map[string]any{"type": "flush"})
		return resp.SimpleString("OK"), true

	case "TIME":
		now := time.Now()
		return resp.Array([]resp.Value{
			resp.BulkString(strconv.FormatInt(now.Unix(), 10)),
			resp.BulkString(strconv.FormatInt(int64(now.Nanosecond()/1000), 10)),
		}), false

	case "SLOWLOG":
		if len(args) > 0 && strings.ToUpper(args[0]) == "LEN" {
			snap := e.Telemetry.GetSnapshot(0)
			return resp.Integer(int64(len(snap.RecentSlowLogs))), false
		}
		if len(args) > 0 && strings.ToUpper(args[0]) == "RESET" {
			return resp.SimpleString("OK"), false
		}
		// SLOWLOG GET [n]
		snap := e.Telemetry.GetSnapshot(0)
		logs := snap.RecentSlowLogs
		items := make([]resp.Value, len(logs))
		for i, l := range logs {
			cmdParts := make([]resp.Value, len(l.Command))
			for j, p := range l.Command {
				cmdParts[j] = resp.BulkString(p)
			}
			items[i] = resp.Array([]resp.Value{
				resp.Integer(l.ID),
				resp.Integer(l.Timestamp),
				resp.Integer(l.DurationMicro),
				resp.Array(cmdParts),
			})
		}
		return resp.Array(items), false

	// ================= Key & String Commands =================
	case "SET":
		return e.handleSet(args)

	case "GET":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'get' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok {
			return resp.Null(), false
		}
		if entry.Type != TypeString {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		return resp.BulkString(entry.Value.(string)), false

	case "MSET":
		if len(args) < 2 || len(args)%2 != 0 {
			return resp.Error("ERR wrong number of arguments for 'mset' command"), false
		}
		for i := 0; i < len(args); i += 2 {
			e.Keyspace.Set(args[i], &Entry{
				Type:  TypeString,
				Value: args[i+1],
			})
		}
		return resp.SimpleString("OK"), true

	case "MGET":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'mget' command"), false
		}
		items := make([]resp.Value, len(args))
		for i, k := range args {
			entry, ok := e.Keyspace.Get(k)
			if !ok || entry.Type != TypeString {
				items[i] = resp.Null()
			} else {
				items[i] = resp.BulkString(entry.Value.(string))
			}
		}
		return resp.Array(items), false

	case "DEL":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'del' command"), false
		}
		count := e.Keyspace.Delete(args...)
		e.Telemetry.BroadcastEvent(map[string]any{"type": "del", "keys": args})
		return resp.Integer(count), true

	case "EXISTS":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'exists' command"), false
		}
		return resp.Integer(e.Keyspace.Exists(args...)), false

	case "TYPE":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'type' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok {
			return resp.SimpleString("none"), false
		}
		return resp.SimpleString(string(entry.Type)), false

	case "INCR":
		return e.handleIncrBy(args, 1)

	case "DECR":
		return e.handleIncrBy(args, -1)

	case "INCRBY":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'incrby' command"), false
		}
		delta, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		return e.handleIncrBy([]string{args[0]}, delta)

	case "DECRBY":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'decrby' command"), false
		}
		delta, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		return e.handleIncrBy([]string{args[0]}, -delta)

	case "APPEND":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'append' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		var newVal string
		if !ok {
			newVal = args[1]
			e.Keyspace.Set(args[0], &Entry{Type: TypeString, Value: newVal})
		} else {
			if entry.Type != TypeString {
				return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
			newVal = entry.Value.(string) + args[1]
			entry.Value = newVal
		}
		return resp.Integer(int64(len(newVal))), true

	case "STRLEN":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'strlen' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok {
			return resp.Integer(0), false
		}
		if entry.Type != TypeString {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		return resp.Integer(int64(len(entry.Value.(string)))), false

	case "SETNX":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'setnx' command"), false
		}
		if _, ok := e.Keyspace.Get(args[0]); ok {
			return resp.Integer(0), false
		}
		e.Keyspace.Set(args[0], &Entry{Type: TypeString, Value: args[1]})
		return resp.Integer(1), true

	case "SETEX":
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'setex' command"), false
		}
		sec, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || sec <= 0 {
			return resp.Error("ERR invalid expire time in 'setex' command"), false
		}
		e.Keyspace.Set(args[0], &Entry{
			Type:      TypeString,
			Value:     args[2],
			ExpiresAt: time.Now().UnixMilli() + sec*1000,
		})
		return resp.SimpleString("OK"), true

	case "PSETEX":
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'psetex' command"), false
		}
		millis, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || millis <= 0 {
			return resp.Error("ERR invalid expire time in 'psetex' command"), false
		}
		e.Keyspace.Set(args[0], &Entry{
			Type:      TypeString,
			Value:     args[2],
			ExpiresAt: time.Now().UnixMilli() + millis,
		})
		return resp.SimpleString("OK"), true

	case "EXPIRE":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'expire' command"), false
		}
		sec, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		if e.Keyspace.Expire(args[0], sec*1000) {
			return resp.Integer(1), true
		}
		return resp.Integer(0), false

	case "PEXPIRE":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'pexpire' command"), false
		}
		millis, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		if e.Keyspace.Expire(args[0], millis) {
			return resp.Integer(1), true
		}
		return resp.Integer(0), false

	case "TTL":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'ttl' command"), false
		}
		return resp.Integer(e.Keyspace.TTL(args[0])), false

	case "PTTL":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'pttl' command"), false
		}
		return resp.Integer(e.Keyspace.PTTL(args[0])), false

	case "PERSIST":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'persist' command"), false
		}
		if e.Keyspace.Persist(args[0]) {
			return resp.Integer(1), true
		}
		return resp.Integer(0), false

	case "KEYS":
		pattern := "*"
		if len(args) > 0 {
			pattern = args[0]
		}
		keys := e.Keyspace.Keys(pattern)
		items := make([]resp.Value, len(keys))
		for i, k := range keys {
			items[i] = resp.BulkString(k)
		}
		return resp.Array(items), false

	// ================= Hash Commands =================
	case "HSET":
		return e.handleHSet(args)

	case "HGET":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'hget' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok {
			return resp.Null(), false
		}
		if entry.Type != TypeHash {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		h := entry.Value.(*datastruct.Hash)
		val, ok := h.Get(args[1])
		if !ok {
			return resp.Null(), false
		}
		return resp.BulkString(val), false

	case "HMSET":
		return e.handleHMSet(args)

	case "HMGET":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'hmget' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		items := make([]resp.Value, len(args)-1)
		if !ok || entry.Type != TypeHash {
			for i := range items {
				items[i] = resp.Null()
			}
			return resp.Array(items), false
		}
		h := entry.Value.(*datastruct.Hash)
		for i := 1; i < len(args); i++ {
			if v, ok := h.Get(args[i]); ok {
				items[i-1] = resp.BulkString(v)
			} else {
				items[i-1] = resp.Null()
			}
		}
		return resp.Array(items), false

	case "HDEL":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'hdel' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeHash {
			return resp.Integer(0), false
		}
		h := entry.Value.(*datastruct.Hash)
		count := h.Del(args[1:]...)
		return resp.Integer(count), true

	case "HEXISTS":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'hexists' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeHash {
			return resp.Integer(0), false
		}
		h := entry.Value.(*datastruct.Hash)
		if h.Exists(args[1]) {
			return resp.Integer(1), false
		}
		return resp.Integer(0), false

	case "HLEN":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'hlen' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeHash {
			return resp.Integer(0), false
		}
		return resp.Integer(entry.Value.(*datastruct.Hash).Len()), false

	case "HKEYS":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'hkeys' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeHash {
			return resp.Array([]resp.Value{}), false
		}
		keys := entry.Value.(*datastruct.Hash).Keys()
		items := make([]resp.Value, len(keys))
		for i, k := range keys {
			items[i] = resp.BulkString(k)
		}
		return resp.Array(items), false

	case "HVALS":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'hvals' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeHash {
			return resp.Array([]resp.Value{}), false
		}
		vals := entry.Value.(*datastruct.Hash).Vals()
		items := make([]resp.Value, len(vals))
		for i, v := range vals {
			items[i] = resp.BulkString(v)
		}
		return resp.Array(items), false

	case "HGETALL":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'hgetall' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeHash {
			return resp.Array([]resp.Value{}), false
		}
		all := entry.Value.(*datastruct.Hash).GetAll()
		items := make([]resp.Value, 0, len(all)*2)
		for f, v := range all {
			items = append(items, resp.BulkString(f), resp.BulkString(v))
		}
		return resp.Array(items), false

	case "HINCRBY":
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'hincrby' command"), false
		}
		delta, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		var h *datastruct.Hash
		if !ok {
			h = datastruct.NewHash()
			e.Keyspace.Set(args[0], &Entry{Type: TypeHash, Value: h})
		} else {
			if entry.Type != TypeHash {
				return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
			h = entry.Value.(*datastruct.Hash)
		}
		val, err := h.IncrBy(args[1], delta)
		if err != nil {
			return resp.Error("ERR hash value is not an integer"), false
		}
		return resp.Integer(val), true

	// ================= List Commands =================
	case "LPUSH":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'lpush' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		var l *datastruct.List
		if !ok {
			l = datastruct.NewList()
			e.Keyspace.Set(args[0], &Entry{Type: TypeList, Value: l})
		} else {
			if entry.Type != TypeList {
				return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
			l = entry.Value.(*datastruct.List)
		}
		length := l.LPush(args[1:]...)
		return resp.Integer(length), true

	case "RPUSH":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'rpush' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		var l *datastruct.List
		if !ok {
			l = datastruct.NewList()
			e.Keyspace.Set(args[0], &Entry{Type: TypeList, Value: l})
		} else {
			if entry.Type != TypeList {
				return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
			l = entry.Value.(*datastruct.List)
		}
		length := l.RPush(args[1:]...)
		return resp.Integer(length), true

	case "LPOP":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'lpop' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeList {
			return resp.Null(), false
		}
		count := 1
		if len(args) > 1 {
			var err error
			count, err = strconv.Atoi(args[1])
			if err != nil {
				return resp.Error("ERR value is not an integer or out of range"), false
			}
		}
		items, popped := entry.Value.(*datastruct.List).LPop(count)
		if !popped {
			return resp.Null(), false
		}
		if len(args) == 1 {
			return resp.BulkString(items[0]), true
		}
		respItems := make([]resp.Value, len(items))
		for i, it := range items {
			respItems[i] = resp.BulkString(it)
		}
		return resp.Array(respItems), true

	case "RPOP":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'rpop' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeList {
			return resp.Null(), false
		}
		count := 1
		if len(args) > 1 {
			var err error
			count, err = strconv.Atoi(args[1])
			if err != nil {
				return resp.Error("ERR value is not an integer or out of range"), false
			}
		}
		items, popped := entry.Value.(*datastruct.List).RPop(count)
		if !popped {
			return resp.Null(), false
		}
		if len(args) == 1 {
			return resp.BulkString(items[0]), true
		}
		respItems := make([]resp.Value, len(items))
		for i, it := range items {
			respItems[i] = resp.BulkString(it)
		}
		return resp.Array(respItems), true

	case "LRANGE":
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'lrange' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeList {
			return resp.Array([]resp.Value{}), false
		}
		start, err1 := strconv.ParseInt(args[1], 10, 64)
		stop, err2 := strconv.ParseInt(args[2], 10, 64)
		if err1 != nil || err2 != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		elements := entry.Value.(*datastruct.List).Range(start, stop)
		items := make([]resp.Value, len(elements))
		for i, it := range elements {
			items[i] = resp.BulkString(it)
		}
		return resp.Array(items), false

	case "LLEN":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'llen' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeList {
			return resp.Integer(0), false
		}
		return resp.Integer(entry.Value.(*datastruct.List).Len()), false

	case "LINDEX":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'lindex' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeList {
			return resp.Null(), false
		}
		idx, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		val, ok := entry.Value.(*datastruct.List).Index(idx)
		if !ok {
			return resp.Null(), false
		}
		return resp.BulkString(val), false

	// ================= Set Commands =================
	case "SADD":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'sadd' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		var s *datastruct.Set
		if !ok {
			s = datastruct.NewSet()
			e.Keyspace.Set(args[0], &Entry{Type: TypeSet, Value: s})
		} else {
			if entry.Type != TypeSet {
				return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
			s = entry.Value.(*datastruct.Set)
		}
		added := s.Add(args[1:]...)
		return resp.Integer(added), true

	case "SREM":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'srem' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeSet {
			return resp.Integer(0), false
		}
		removed := entry.Value.(*datastruct.Set).Rem(args[1:]...)
		return resp.Integer(removed), true

	case "SMEMBERS":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'smembers' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeSet {
			return resp.Array([]resp.Value{}), false
		}
		members := entry.Value.(*datastruct.Set).Members()
		items := make([]resp.Value, len(members))
		for i, m := range members {
			items[i] = resp.BulkString(m)
		}
		return resp.Array(items), false

	case "SISMEMBER":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'sismember' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeSet {
			return resp.Integer(0), false
		}
		if entry.Value.(*datastruct.Set).IsMember(args[1]) {
			return resp.Integer(1), false
		}
		return resp.Integer(0), false

	case "SCARD":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'scard' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeSet {
			return resp.Integer(0), false
		}
		return resp.Integer(entry.Value.(*datastruct.Set).Card()), false

	// ================= Sorted Set (ZSet) Commands =================
	case "ZADD":
		return e.handleZAdd(args)

	case "ZSCORE":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'zscore' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeZSet {
			return resp.Null(), false
		}
		sl := entry.Value.(*datastruct.SkipList)
		score, found := sl.GetScore(args[1])
		if !found {
			return resp.Null(), false
		}
		return resp.BulkString(strconv.FormatFloat(score, 'f', -1, 64)), false

	case "ZRANK":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'zrank' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeZSet {
			return resp.Null(), false
		}
		rank := entry.Value.(*datastruct.SkipList).GetRank(args[1])
		if rank < 0 {
			return resp.Null(), false
		}
		return resp.Integer(rank), false

	case "ZRANGE":
		return e.handleZRange(args, false)

	case "ZREVRANGE":
		return e.handleZRange(args, true)

	case "ZREM":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'zrem' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeZSet {
			return resp.Integer(0), false
		}
		sl := entry.Value.(*datastruct.SkipList)
		var count int64
		for _, m := range args[1:] {
			if sl.Delete(m) {
				count++
			}
		}
		return resp.Integer(count), true

	case "ZCARD":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'zcard' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeZSet {
			return resp.Integer(0), false
		}
		return resp.Integer(entry.Value.(*datastruct.SkipList).Length), false

	// ================= Stream Commands =================
	case "XADD":
		return e.handleXAdd(args)

	case "XRANGE":
		return e.handleXRange(args)

	case "XREVRANGE":
		return e.handleXRevRange(args)

	case "XLEN":
		if len(args) < 1 {
			return resp.Error("ERR wrong number of arguments for 'xlen' command"), false
		}
		entry, ok := e.Keyspace.Get(args[0])
		if !ok || entry.Type != TypeStream {
			return resp.Integer(0), false
		}
		return resp.Integer(entry.Value.(*datastruct.Stream).Len()), false

	case "XDEL":
		return e.handleXDel(args)

	case "XTRIM":
		return e.handleXTrim(args)

	case "XREAD":
		return e.handleXRead(args)

	case "XGROUP":
		return e.handleXGroup(args)

	case "XREADGROUP":
		return e.handleXReadGroup(args)

	case "XACK":
		return e.handleXAck(args)

	case "XPENDING":
		return e.handleXPending(args)

	case "XINFO":
		return e.handleXInfo(args)

	// ================= AI Vector Commands (Next-Gen) =================
	case "VADD":
		return e.handleVAdd(args)

	case "VSEARCH":
		return e.handleVSearch(args)

	case "VSIM":
		return e.handleVSim(args)

	// ================= Pub/Sub Commands =================
	case "PUBLISH":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'publish' command"), false
		}
		channel := args[0]
		message := []byte(args[1])
		delivered := e.Broker.Publish(channel, message)
		e.Telemetry.BroadcastEvent(map[string]any{
			"type":      "pubsub",
			"channel":   channel,
			"message":   args[1],
			"delivered": delivered,
		})
		return resp.Integer(delivered), false

	default:
		return resp.Error(fmt.Sprintf("ERR unknown command '%s'", cmd)), false
	}
}

// Helpers for SET command (supports EX, PX, NX, XX)
func (e *Engine) handleSet(args []string) (resp.Value, bool) {
	if len(args) < 2 {
		return resp.Error("ERR wrong number of arguments for 'set' command"), false
	}

	key := args[0]
	val := args[1]

	var ttlMillis int64
	nx := false
	xx := false

	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "EX":
			if i+1 >= len(args) {
				return resp.Error("ERR syntax error"), false
			}
			sec, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || sec <= 0 {
				return resp.Error("ERR value is not an integer or out of range"), false
			}
			ttlMillis = sec * 1000
			i++
		case "PX":
			if i+1 >= len(args) {
				return resp.Error("ERR syntax error"), false
			}
			m, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || m <= 0 {
				return resp.Error("ERR value is not an integer or out of range"), false
			}
			ttlMillis = m
			i++
		case "NX":
			nx = true
		case "XX":
			xx = true
		}
	}

	existing, exists := e.Keyspace.Get(key)
	if nx && exists {
		return resp.Null(), false
	}
	if xx && !exists {
		return resp.Null(), false
	}

	var expiresAt int64
	if ttlMillis > 0 {
		expiresAt = time.Now().UnixMilli() + ttlMillis
	} else if existing != nil && existing.ExpiresAt > 0 {
		// Redis SET without TTL option resets TTL unless KEEPTTL is passed
	}

	e.Keyspace.Set(key, &Entry{
		Type:      TypeString,
		Value:     val,
		ExpiresAt: expiresAt,
	})

	e.Telemetry.BroadcastEvent(map[string]any{
		"type": "set",
		"key":  key,
	})

	return resp.SimpleString("OK"), true
}

func (e *Engine) handleIncrBy(args []string, delta int64) (resp.Value, bool) {
	if len(args) < 1 {
		return resp.Error("ERR wrong number of arguments for incr/decr command"), false
	}
	key := args[0]
	entry, ok := e.Keyspace.Get(key)
	var current int64 = 0
	if ok {
		if entry.Type != TypeString {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		var err error
		current, err = strconv.ParseInt(entry.Value.(string), 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
	}

	newVal := current + delta
	strVal := strconv.FormatInt(newVal, 10)
	if ok {
		entry.Value = strVal
	} else {
		e.Keyspace.Set(key, &Entry{Type: TypeString, Value: strVal})
	}

	return resp.Integer(newVal), true
}

func (e *Engine) handleHSet(args []string) (resp.Value, bool) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.Error("ERR wrong number of arguments for 'hset' command"), false
	}
	key := args[0]
	entry, ok := e.Keyspace.Get(key)
	var h *datastruct.Hash
	if !ok {
		h = datastruct.NewHash()
		e.Keyspace.Set(key, &Entry{Type: TypeHash, Value: h})
	} else {
		if entry.Type != TypeHash {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		h = entry.Value.(*datastruct.Hash)
	}

	var addedCount int64
	for i := 1; i < len(args); i += 2 {
		addedCount += h.Set(args[i], args[i+1])
	}
	return resp.Integer(addedCount), true
}

func (e *Engine) handleHMSet(args []string) (resp.Value, bool) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.Error("ERR wrong number of arguments for 'hmset' command"), false
	}
	key := args[0]
	entry, ok := e.Keyspace.Get(key)
	var h *datastruct.Hash
	if !ok {
		h = datastruct.NewHash()
		e.Keyspace.Set(key, &Entry{Type: TypeHash, Value: h})
	} else {
		if entry.Type != TypeHash {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		h = entry.Value.(*datastruct.Hash)
	}

	kvs := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		kvs[args[i]] = args[i+1]
	}
	h.MSet(kvs)
	return resp.SimpleString("OK"), true
}

func (e *Engine) handleZAdd(args []string) (resp.Value, bool) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.Error("ERR wrong number of arguments for 'zadd' command"), false
	}
	key := args[0]
	entry, ok := e.Keyspace.Get(key)
	var sl *datastruct.SkipList
	if !ok {
		sl = datastruct.NewSkipList()
		e.Keyspace.Set(key, &Entry{Type: TypeZSet, Value: sl})
	} else {
		if entry.Type != TypeZSet {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		sl = entry.Value.(*datastruct.SkipList)
	}

	var addedCount int64
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return resp.Error("ERR value is not a valid float"), false
		}
		member := args[i+1]
		if _, exists := sl.GetScore(member); !exists {
			addedCount++
		}
		sl.Insert(score, member)
	}

	return resp.Integer(addedCount), true
}

func (e *Engine) handleZRange(args []string, reverse bool) (resp.Value, bool) {
	if len(args) < 3 {
		return resp.Error("ERR wrong number of arguments for 'zrange' command"), false
	}
	entry, ok := e.Keyspace.Get(args[0])
	if !ok || entry.Type != TypeZSet {
		return resp.Array([]resp.Value{}), false
	}

	start, err1 := strconv.ParseInt(args[1], 10, 64)
	stop, err2 := strconv.ParseInt(args[2], 10, 64)
	if err1 != nil || err2 != nil {
		return resp.Error("ERR value is not an integer or out of range"), false
	}

	withScores := false
	if len(args) > 3 && strings.ToUpper(args[3]) == "WITHSCORES" {
		withScores = true
	}

	sl := entry.Value.(*datastruct.SkipList)
	scored := sl.Range(start, stop, reverse)

	var items []resp.Value
	for _, sm := range scored {
		items = append(items, resp.BulkString(sm.Member))
		if withScores {
			items = append(items, resp.BulkString(strconv.FormatFloat(sm.Score, 'f', -1, 64)))
		}
	}
	return resp.Array(items), false
}

func (e *Engine) handleXAdd(args []string) (resp.Value, bool) {
	if len(args) < 4 {
		return resp.Error("ERR wrong number of arguments for 'xadd' command"), false
	}
	key := args[0]
	idx := 1
	var maxlen int64 = -1

	if strings.ToUpper(args[idx]) == "MAXLEN" {
		idx++
		if idx < len(args) && args[idx] == "~" {
			idx++
		}
		if idx >= len(args) {
			return resp.Error("ERR syntax error"), false
		}
		ml, err := strconv.ParseInt(args[idx], 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		maxlen = ml
		idx++
	}

	if idx >= len(args) {
		return resp.Error("ERR wrong number of arguments for 'xadd' command"), false
	}
	idArg := args[idx]
	idx++

	fieldsList := args[idx:]
	if len(fieldsList) == 0 || len(fieldsList)%2 != 0 {
		return resp.Error("ERR wrong number of arguments for 'xadd' command"), false
	}

	entry, ok := e.Keyspace.Get(key)
	var s *datastruct.Stream
	if !ok {
		s = datastruct.NewStream()
		e.Keyspace.Set(key, &Entry{Type: TypeStream, Value: s})
	} else {
		if entry.Type != TypeStream {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		s = entry.Value.(*datastruct.Stream)
	}

	fields := make(map[string]string)
	order := make([]string, 0, len(fieldsList)/2)
	for i := 0; i < len(fieldsList); i += 2 {
		f := fieldsList[i]
		v := fieldsList[i+1]
		fields[f] = v
		order = append(order, f)
	}

	assignedID, err := s.Add(idArg, fields, order)
	if err != nil {
		return resp.Error(err.Error()), false
	}

	if maxlen >= 0 {
		s.Trim(maxlen)
	}

	return resp.BulkString(assignedID), true
}

func (e *Engine) handleXRange(args []string) (resp.Value, bool) {
	if len(args) < 3 {
		return resp.Error("ERR wrong number of arguments for 'xrange' command"), false
	}
	key := args[0]
	start := args[1]
	end := args[2]

	var count int64 = -1
	if len(args) >= 5 && strings.ToUpper(args[3]) == "COUNT" {
		c, err := strconv.ParseInt(args[4], 10, 64)
		if err == nil {
			count = c
		}
	}

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeStream {
		return resp.Array([]resp.Value{}), false
	}

	s := entry.Value.(*datastruct.Stream)
	entries := s.Range(start, end, count)

	results := make([]resp.Value, len(entries))
	for i, en := range entries {
		fieldPairs := make([]resp.Value, 0, len(en.Order)*2)
		for _, f := range en.Order {
			fieldPairs = append(fieldPairs, resp.BulkString(f), resp.BulkString(en.Fields[f]))
		}
		results[i] = resp.Array([]resp.Value{
			resp.BulkString(en.ID),
			resp.Array(fieldPairs),
		})
	}

	return resp.Array(results), false
}

func (e *Engine) handleXRevRange(args []string) (resp.Value, bool) {
	if len(args) < 3 {
		return resp.Error("ERR wrong number of arguments for 'xrevrange' command"), false
	}
	key := args[0]
	end := args[1]
	start := args[2]

	var count int64 = -1
	if len(args) >= 5 && strings.ToUpper(args[3]) == "COUNT" {
		c, err := strconv.ParseInt(args[4], 10, 64)
		if err == nil {
			count = c
		}
	}

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeStream {
		return resp.Array([]resp.Value{}), false
	}

	s := entry.Value.(*datastruct.Stream)
	entries := s.RevRange(end, start, count)

	results := make([]resp.Value, len(entries))
	for i, en := range entries {
		fieldPairs := make([]resp.Value, 0, len(en.Order)*2)
		for _, f := range en.Order {
			fieldPairs = append(fieldPairs, resp.BulkString(f), resp.BulkString(en.Fields[f]))
		}
		results[i] = resp.Array([]resp.Value{
			resp.BulkString(en.ID),
			resp.Array(fieldPairs),
		})
	}

	return resp.Array(results), false
}

func (e *Engine) handleXDel(args []string) (resp.Value, bool) {
	if len(args) < 2 {
		return resp.Error("ERR wrong number of arguments for 'xdel' command"), false
	}
	key := args[0]
	ids := args[1:]

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeStream {
		return resp.Integer(0), false
	}

	s := entry.Value.(*datastruct.Stream)
	deleted := s.Delete(ids)
	return resp.Integer(deleted), true
}

func (e *Engine) handleXTrim(args []string) (resp.Value, bool) {
	if len(args) < 3 {
		return resp.Error("ERR wrong number of arguments for 'xtrim' command"), false
	}
	key := args[0]
	if strings.ToUpper(args[1]) != "MAXLEN" {
		return resp.Error("ERR syntax error, expected MAXLEN"), false
	}
	countIdx := 2
	if countIdx < len(args) && args[countIdx] == "~" {
		countIdx = 3
	}
	if countIdx >= len(args) {
		return resp.Error("ERR syntax error"), false
	}
	count, err := strconv.ParseInt(args[countIdx], 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range"), false
	}

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeStream {
		return resp.Integer(0), false
	}

	s := entry.Value.(*datastruct.Stream)
	evicted := s.Trim(count)
	return resp.Integer(evicted), true
}

func (e *Engine) handleXRead(args []string) (resp.Value, bool) {
	var count int64 = -1
	var blockMs int64 = -1
	streamsIdx := -1

	for i := 0; i < len(args); i++ {
		upper := strings.ToUpper(args[i])
		if upper == "COUNT" && i+1 < len(args) {
			c, err := strconv.ParseInt(args[i+1], 10, 64)
			if err == nil {
				count = c
			}
			i++
		} else if upper == "BLOCK" && i+1 < len(args) {
			b, err := strconv.ParseInt(args[i+1], 10, 64)
			if err == nil {
				blockMs = b
			}
			i++
		} else if upper == "STREAMS" {
			streamsIdx = i + 1
			break
		}
	}

	if streamsIdx == -1 {
		return resp.Error("ERR syntax error, missing STREAMS in 'xread'"), false
	}

	remaining := args[streamsIdx:]
	if len(remaining) < 2 || len(remaining)%2 != 0 {
		return resp.Error("ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified"), false
	}

	numStreams := len(remaining) / 2
	keys := remaining[:numStreams]
	ids := remaining[numStreams:]

	actualIDs := make([]string, len(ids))
	for idx, id := range ids {
		if id == "$" {
			entry, ok := e.Keyspace.Get(keys[idx])
			if ok && entry.Type == TypeStream {
				actualIDs[idx] = entry.Value.(*datastruct.Stream).LastID()
			} else {
				actualIDs[idx] = "0-0"
			}
		} else {
			actualIDs[idx] = id
		}
	}

	readOnce := func() []resp.Value {
		var streamResults []resp.Value
		for idx, key := range keys {
			lastID := actualIDs[idx]
			entry, ok := e.Keyspace.Get(key)
			if !ok || entry.Type != TypeStream {
				continue
			}
			s := entry.Value.(*datastruct.Stream)
			entries := s.Read(lastID, count)
			if len(entries) == 0 {
				continue
			}

			entryValues := make([]resp.Value, len(entries))
			for i, en := range entries {
				fieldPairs := make([]resp.Value, 0, len(en.Order)*2)
				for _, f := range en.Order {
					fieldPairs = append(fieldPairs, resp.BulkString(f), resp.BulkString(en.Fields[f]))
				}
				entryValues[i] = resp.Array([]resp.Value{
					resp.BulkString(en.ID),
					resp.Array(fieldPairs),
				})
			}

			streamResults = append(streamResults, resp.Array([]resp.Value{
				resp.BulkString(key),
				resp.Array(entryValues),
			}))
		}
		return streamResults
	}

	results := readOnce()
	if len(results) > 0 || blockMs < 0 {
		if len(results) == 0 {
			return resp.NullArray(), false
		}
		return resp.Array(results), false
	}

	var deadline time.Time
	if blockMs > 0 {
		deadline = time.Now().Add(time.Duration(blockMs) * time.Millisecond)
	}

	for {
		var cancelFuncs []func()
		var waitChannels []<-chan struct{}

		for _, key := range keys {
			entry, ok := e.Keyspace.Get(key)
			if ok && entry.Type == TypeStream {
				s := entry.Value.(*datastruct.Stream)
				ch, cancel := s.RegisterWaiter()
				waitChannels = append(waitChannels, ch)
				cancelFuncs = append(cancelFuncs, cancel)
			}
		}

		selectTriggered := false
		if len(waitChannels) > 0 {
			for _, ch := range waitChannels {
				select {
				case <-ch:
					selectTriggered = true
				default:
				}
			}
		}

		if !selectTriggered {
			time.Sleep(10 * time.Millisecond)
		}

		for _, c := range cancelFuncs {
			c()
		}

		results = readOnce()
		if len(results) > 0 {
			return resp.Array(results), false
		}

		if blockMs > 0 && time.Now().After(deadline) {
			return resp.NullArray(), false
		}
	}
}

func (e *Engine) handleXGroup(args []string) (resp.Value, bool) {
	if len(args) < 2 {
		return resp.Error("ERR wrong number of arguments for 'xgroup' command"), false
	}
	subCmd := strings.ToUpper(args[0])

	switch subCmd {
	case "CREATE":
		if len(args) < 4 {
			return resp.Error("ERR wrong number of arguments for 'xgroup create' command"), false
		}
		key := args[1]
		group := args[2]
		id := args[3]
		mkstream := false
		if len(args) >= 5 && strings.ToUpper(args[4]) == "MKSTREAM" {
			mkstream = true
		}

		entry, ok := e.Keyspace.Get(key)
		var s *datastruct.Stream
		if !ok {
			if !mkstream {
				return resp.Error("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you can specify MKSTREAM to create an empty stream automatically."), false
			}
			s = datastruct.NewStream()
			e.Keyspace.Set(key, &Entry{Type: TypeStream, Value: s})
		} else {
			if entry.Type != TypeStream {
				return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
			}
			s = entry.Value.(*datastruct.Stream)
		}

		err := s.CreateGroup(group, id)
		if err != nil {
			return resp.Error(err.Error()), false
		}
		return resp.SimpleString("OK"), true

	case "DESTROY":
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'xgroup destroy' command"), false
		}
		key := args[1]
		group := args[2]
		entry, ok := e.Keyspace.Get(key)
		if !ok || entry.Type != TypeStream {
			return resp.Integer(0), false
		}
		s := entry.Value.(*datastruct.Stream)
		destroyed := s.DestroyGroup(group)
		if destroyed {
			return resp.Integer(1), true
		}
		return resp.Integer(0), false

	case "SETID":
		if len(args) < 4 {
			return resp.Error("ERR wrong number of arguments for 'xgroup setid' command"), false
		}
		key := args[1]
		group := args[2]
		id := args[3]
		entry, ok := e.Keyspace.Get(key)
		if !ok || entry.Type != TypeStream {
			return resp.Error("ERR no such key"), false
		}
		s := entry.Value.(*datastruct.Stream)
		if err := s.SetGroupID(group, id); err != nil {
			return resp.Error(err.Error()), false
		}
		return resp.SimpleString("OK"), true

	case "DELCONSUMER":
		if len(args) < 4 {
			return resp.Error("ERR wrong number of arguments for 'xgroup delconsumer' command"), false
		}
		key := args[1]
		group := args[2]
		consumer := args[3]
		entry, ok := e.Keyspace.Get(key)
		if !ok || entry.Type != TypeStream {
			return resp.Integer(0), false
		}
		s := entry.Value.(*datastruct.Stream)
		pelCount := s.DeleteConsumer(group, consumer)
		return resp.Integer(pelCount), true

	default:
		return resp.Error(fmt.Sprintf("ERR unknown XGROUP subcommand '%s'", subCmd)), false
	}
}

func (e *Engine) handleXReadGroup(args []string) (resp.Value, bool) {
	if len(args) < 6 || strings.ToUpper(args[0]) != "GROUP" {
		return resp.Error("ERR syntax error, expected XREADGROUP GROUP <group> <consumer> ..."), false
	}
	group := args[1]
	consumer := args[2]

	var count int64 = -1
	var blockMs int64 = -1
	noAck := false
	streamsIdx := -1

	for i := 3; i < len(args); i++ {
		upper := strings.ToUpper(args[i])
		if upper == "COUNT" && i+1 < len(args) {
			c, err := strconv.ParseInt(args[i+1], 10, 64)
			if err == nil {
				count = c
			}
			i++
		} else if upper == "BLOCK" && i+1 < len(args) {
			b, err := strconv.ParseInt(args[i+1], 10, 64)
			if err == nil {
				blockMs = b
			}
			i++
		} else if upper == "NOACK" {
			noAck = true
		} else if upper == "STREAMS" {
			streamsIdx = i + 1
			break
		}
	}

	if streamsIdx == -1 {
		return resp.Error("ERR syntax error, missing STREAMS in 'xreadgroup'"), false
	}

	remaining := args[streamsIdx:]
	if len(remaining) < 2 || len(remaining)%2 != 0 {
		return resp.Error("ERR Unbalanced XREADGROUP list of streams"), false
	}

	numStreams := len(remaining) / 2
	keys := remaining[:numStreams]
	ids := remaining[numStreams:]

	readOnce := func() ([]resp.Value, error) {
		var streamResults []resp.Value
		for idx, key := range keys {
			id := ids[idx]
			entry, ok := e.Keyspace.Get(key)
			if !ok || entry.Type != TypeStream {
				continue
			}
			s := entry.Value.(*datastruct.Stream)
			entries, err := s.ReadGroup(group, consumer, id, count, noAck)
			if err != nil {
				return nil, err
			}
			if len(entries) == 0 {
				continue
			}

			entryValues := make([]resp.Value, len(entries))
			for i, en := range entries {
				fieldPairs := make([]resp.Value, 0, len(en.Order)*2)
				for _, f := range en.Order {
					fieldPairs = append(fieldPairs, resp.BulkString(f), resp.BulkString(en.Fields[f]))
				}
				entryValues[i] = resp.Array([]resp.Value{
					resp.BulkString(en.ID),
					resp.Array(fieldPairs),
				})
			}

			streamResults = append(streamResults, resp.Array([]resp.Value{
				resp.BulkString(key),
				resp.Array(entryValues),
			}))
		}
		return streamResults, nil
	}

	results, err := readOnce()
	if err != nil {
		return resp.Error(err.Error()), false
	}
	if len(results) > 0 || blockMs < 0 {
		if len(results) == 0 {
			return resp.NullArray(), false
		}
		return resp.Array(results), false
	}

	var deadline time.Time
	if blockMs > 0 {
		deadline = time.Now().Add(time.Duration(blockMs) * time.Millisecond)
	}

	for {
		var cancelFuncs []func()
		var waitChannels []<-chan struct{}

		for _, key := range keys {
			entry, ok := e.Keyspace.Get(key)
			if ok && entry.Type == TypeStream {
				s := entry.Value.(*datastruct.Stream)
				ch, cancel := s.RegisterWaiter()
				waitChannels = append(waitChannels, ch)
				cancelFuncs = append(cancelFuncs, cancel)
			}
		}

		selectTriggered := false
		if len(waitChannels) > 0 {
			for _, ch := range waitChannels {
				select {
				case <-ch:
					selectTriggered = true
				default:
				}
			}
		}

		if !selectTriggered {
			time.Sleep(10 * time.Millisecond)
		}

		for _, c := range cancelFuncs {
			c()
		}

		results, err = readOnce()
		if err != nil {
			return resp.Error(err.Error()), false
		}
		if len(results) > 0 {
			return resp.Array(results), false
		}

		if blockMs > 0 && time.Now().After(deadline) {
			return resp.NullArray(), false
		}
	}
}

func (e *Engine) handleXAck(args []string) (resp.Value, bool) {
	if len(args) < 3 {
		return resp.Error("ERR wrong number of arguments for 'xack' command"), false
	}
	key := args[0]
	group := args[1]
	ids := args[2:]

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeStream {
		return resp.Integer(0), false
	}

	s := entry.Value.(*datastruct.Stream)
	acked := s.Ack(group, ids)
	return resp.Integer(acked), true
}

func (e *Engine) handleXPending(args []string) (resp.Value, bool) {
	if len(args) < 2 {
		return resp.Error("ERR wrong number of arguments for 'xpending' command"), false
	}
	key := args[0]
	group := args[1]

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeStream {
		return resp.Error("ERR no such key"), false
	}
	s := entry.Value.(*datastruct.Stream)

	if len(args) == 2 {
		total, minID, maxID, counts, err := s.GetPendingSummary(group)
		if err != nil {
			return resp.Error(err.Error()), false
		}
		if total == 0 {
			return resp.Array([]resp.Value{
				resp.Integer(0),
				resp.Null(),
				resp.Null(),
				resp.Null(),
			}), false
		}

		var consumerVals []resp.Value
		for cName, cCount := range counts {
			consumerVals = append(consumerVals, resp.Array([]resp.Value{
				resp.BulkString(cName),
				resp.Integer(cCount),
			}))
		}

		return resp.Array([]resp.Value{
			resp.Integer(total),
			resp.BulkString(minID),
			resp.BulkString(maxID),
			resp.Array(consumerVals),
		}), false
	}

	idx := 2
	var minIdle time.Duration
	if idx < len(args) && strings.ToUpper(args[idx]) == "IDLE" {
		if idx+1 >= len(args) {
			return resp.Error("ERR syntax error"), false
		}
		ms, err := strconv.ParseInt(args[idx+1], 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range"), false
		}
		minIdle = time.Duration(ms) * time.Millisecond
		idx += 2
	}

	if idx+2 >= len(args) {
		return resp.Error("ERR wrong number of arguments for 'xpending' detailed command"), false
	}
	start := args[idx]
	end := args[idx+1]
	count, err := strconv.ParseInt(args[idx+2], 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range"), false
	}
	idx += 3

	consumerFilter := ""
	if idx < len(args) {
		consumerFilter = args[idx]
	}

	detailed, err := s.GetPendingDetailed(group, start, end, count, consumerFilter, minIdle)
	if err != nil {
		return resp.Error(err.Error()), false
	}

	now := time.Now()
	res := make([]resp.Value, len(detailed))
	for i, pe := range detailed {
		idleMs := now.Sub(pe.DeliveryTime).Milliseconds()
		if idleMs < 0 {
			idleMs = 0
		}
		res[i] = resp.Array([]resp.Value{
			resp.BulkString(pe.ID),
			resp.BulkString(pe.ConsumerName),
			resp.Integer(idleMs),
			resp.Integer(pe.DeliveryCount),
		})
	}
	return resp.Array(res), false
}

func (e *Engine) handleXInfo(args []string) (resp.Value, bool) {
	if len(args) < 2 {
		return resp.Error("ERR wrong number of arguments for 'xinfo' command"), false
	}
	subCmd := strings.ToUpper(args[0])
	key := args[1]

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeStream {
		return resp.Error("ERR no such key"), false
	}
	s := entry.Value.(*datastruct.Stream)

	switch subCmd {
	case "STREAM":
		entries := s.Range("-", "+", 1)
		firstID := resp.Null()
		if len(entries) > 0 {
			firstID = resp.BulkString(entries[0].ID)
		}
		lastID := resp.Null()
		if s.Len() > 0 {
			lastID = resp.BulkString(s.LastID())
		}
		groups := s.GetGroupsInfo()

		return resp.Array([]resp.Value{
			resp.BulkString("length"), resp.Integer(s.Len()),
			resp.BulkString("radix-tree-keys"), resp.Integer(s.Len()),
			resp.BulkString("radix-tree-nodes"), resp.Integer(1),
			resp.BulkString("last-generated-id"), lastID,
			resp.BulkString("groups"), resp.Integer(int64(len(groups))),
			resp.BulkString("first-entry"), firstID,
			resp.BulkString("last-entry"), lastID,
		}), false

	case "GROUPS":
		groups := s.GetGroupsInfo()
		res := make([]resp.Value, len(groups))
		for i, g := range groups {
			res[i] = resp.Array([]resp.Value{
				resp.BulkString("name"), resp.BulkString(g["name"].(string)),
				resp.BulkString("consumers"), resp.Integer(int64(g["consumers"].(int))),
				resp.BulkString("pending"), resp.Integer(int64(g["pending"].(int))),
				resp.BulkString("last-delivered-id"), resp.BulkString(g["last-delivered-id"].(string)),
			})
		}
		return resp.Array(res), false

	case "CONSUMERS":
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'xinfo consumers' command"), false
		}
		group := args[2]
		consumers, err := s.GetConsumersInfo(group)
		if err != nil {
			return resp.Error(err.Error()), false
		}
		res := make([]resp.Value, len(consumers))
		for i, c := range consumers {
			res[i] = resp.Array([]resp.Value{
				resp.BulkString("name"), resp.BulkString(c["name"].(string)),
				resp.BulkString("pending"), resp.Integer(int64(c["pending"].(int))),
				resp.BulkString("idle"), resp.Integer(c["idle"].(int64)),
			})
		}
		return resp.Array(res), false

	default:
		return resp.Error(fmt.Sprintf("ERR unknown XINFO subcommand '%s'", subCmd)), false
	}
}

// VADD key id f1 f2 ... fn
func (e *Engine) handleVAdd(args []string) (resp.Value, bool) {
	if len(args) < 3 {
		return resp.Error("ERR wrong number of arguments for 'vadd' command: VADD key id f1 f2 ..."), false
	}
	key := args[0]
	id := args[1]

	dim := len(args) - 2
	floats := make([]float32, dim)
	for i := 2; i < len(args); i++ {
		f, err := strconv.ParseFloat(args[i], 32)
		if err != nil {
			return resp.Error(fmt.Sprintf("ERR invalid float in vector at position %d", i-1)), false
		}
		floats[i-2] = float32(f)
	}

	entry, ok := e.Keyspace.Get(key)
	var vi *datastruct.VectorIndex
	if !ok {
		vi = datastruct.NewVectorIndex(dim)
		e.Keyspace.Set(key, &Entry{Type: TypeVector, Value: vi})
	} else {
		if entry.Type != TypeVector {
			return resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"), false
		}
		vi = entry.Value.(*datastruct.VectorIndex)
	}

	if err := vi.Add(id, floats); err != nil {
		return resp.Error("ERR " + err.Error()), false
	}

	return resp.SimpleString("OK"), true
}

// VSEARCH key top_k metric f1 f2 ... fn
func (e *Engine) handleVSearch(args []string) (resp.Value, bool) {
	if len(args) < 4 {
		return resp.Error("ERR wrong number of arguments for 'vsearch': VSEARCH key top_k metric f1 f2 ..."), false
	}
	key := args[0]
	topK, err := strconv.Atoi(args[1])
	if err != nil || topK <= 0 {
		return resp.Error("ERR invalid top_k"), false
	}
	metric := strings.ToLower(args[2])

	query := make([]float32, len(args)-3)
	for i := 3; i < len(args); i++ {
		f, err := strconv.ParseFloat(args[i], 32)
		if err != nil {
			return resp.Error(fmt.Sprintf("ERR invalid float in query vector at position %d", i-2)), false
		}
		query[i-3] = float32(f)
	}

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeVector {
		return resp.Array([]resp.Value{}), false
	}

	vi := entry.Value.(*datastruct.VectorIndex)
	results, err := vi.Search(query, topK, metric)
	if err != nil {
		return resp.Error("ERR " + err.Error()), false
	}

	items := make([]resp.Value, len(results))
	for i, r := range results {
		items[i] = resp.Array([]resp.Value{
			resp.BulkString(r.ID),
			resp.BulkString(strconv.FormatFloat(r.Score, 'f', 6, 64)),
		})
	}

	return resp.Array(items), false
}

// VSIM key id1 id2 metric
func (e *Engine) handleVSim(args []string) (resp.Value, bool) {
	if len(args) < 3 {
		return resp.Error("ERR wrong number of arguments for 'vsim': VSIM key id1 id2 [metric]"), false
	}
	key := args[0]
	id1 := args[1]
	id2 := args[2]
	metric := "cosine"
	if len(args) > 3 {
		metric = strings.ToLower(args[3])
	}

	entry, ok := e.Keyspace.Get(key)
	if !ok || entry.Type != TypeVector {
		return resp.Null(), false
	}
	vi := entry.Value.(*datastruct.VectorIndex)
	v1, ok1 := vi.Get(id1)
	v2, ok2 := vi.Get(id2)
	if !ok1 || !ok2 {
		return resp.Error("ERR one or both vectors not found"), false
	}

	var score float64
	if metric == "l2" || metric == "euclidean" {
		score = datastruct.EuclideanDistance(v1, v2)
	} else {
		score = datastruct.CosineSimilarity(v1, v2)
	}

	return resp.BulkString(strconv.FormatFloat(score, 'f', 6, 64)), false
}

func extractCommandKeys(cmd string, args []string) []string {
	if len(args) == 0 {
		return nil
	}
	switch cmd {
	case "AUTH", "PING", "ECHO", "QUIT", "COMMAND", "CLIENT", "SELECT", "INFO", "DBSIZE",
		"TIME", "SLOWLOG", "MULTI", "EXEC", "DISCARD", "BGREWRITEAOF", "ACL",
		"FLUSHDB", "FLUSHALL", "CONFIG", "SHUTDOWN", "PUBSUB", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE":
		return nil
	case "MGET", "DEL", "EXISTS":
		return args
	case "MSET":
		var keys []string
		for i := 0; i < len(args); i += 2 {
			keys = append(keys, args[i])
		}
		return keys
	default:
		return []string{args[0]}
	}
}

func (e *Engine) handleACLCommand(connID string, args []string) (resp.Value, bool) {
	if len(args) < 1 {
		return resp.Error("ERR wrong number of arguments for 'acl' command"), false
	}
	if e.ACL == nil {
		return resp.Error("ERR ACL subsystem unavailable"), false
	}

	aclSub := strings.ToUpper(args[0])
	session := e.GetClientSession(connID)
	if aclSub != "WHOAMI" {
		callerName := session.Username
		if callerName == "" {
			callerName = "default"
		}
		caller, ok := e.ACL.GetUser(callerName)
		if !ok || !caller.IsAdmin {
			return resp.Error(fmt.Sprintf("NOPERM administrative command 'ACL %s' not allowed for user '%s'", aclSub, callerName)), false
		}
	}

	switch aclSub {
	case "WHOAMI":
		u := session.Username
		if u == "" {
			u = "default"
		}
		return resp.BulkString(u), false

	case "USERS":
		names := e.ACL.ListUsernames()
		items := make([]resp.Value, len(names))
		for i, n := range names {
			items[i] = resp.BulkString(n)
		}
		return resp.Array(items), false

	case "LIST":
		users := e.ACL.ListUsers()
		items := make([]resp.Value, len(users))
		for i, u := range users {
			state := "off"
			if u.Enabled {
				state = "on"
			}
			perms := "+@all"
			if u.ReadOnly {
				perms = "+@read"
			}
			patterns := strings.Join(u.KeyPatterns, " ~")
			if patterns != "" {
				patterns = "~" + patterns
			}
			line := fmt.Sprintf("user %s %s %s %s", u.Username, state, patterns, perms)
			items[i] = resp.BulkString(line)
		}
		return resp.Array(items), false

	case "GETUSER":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'acl getuser' command"), false
		}
		u, exists := e.ACL.GetUser(args[1])
		if !exists {
			return resp.Null(), false
		}
		var entries []resp.Value
		// flags
		entries = append(entries, resp.BulkString("flags"))
		flagArr := []resp.Value{resp.BulkString("on")}
		if !u.Enabled {
			flagArr = []resp.Value{resp.BulkString("off")}
		}
		if u.IsAdmin {
			flagArr = append(flagArr, resp.BulkString("admin"))
		}
		entries = append(entries, resp.Array(flagArr))
		// keys
		entries = append(entries, resp.BulkString("keys"))
		keyArr := make([]resp.Value, len(u.KeyPatterns))
		for i, kp := range u.KeyPatterns {
			keyArr[i] = resp.BulkString(kp)
		}
		entries = append(entries, resp.Array(keyArr))
		return resp.Array(entries), false

	case "SETUSER":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'acl setuser' command"), false
		}
		username := args[1]
		user, exists := e.ACL.GetUser(username)
		if !exists {
			user = &ACLUser{
				Username:    username,
				Enabled:     true,
				Role:        RoleReadWrite,
				KeyPatterns: []string{"*"},
			}
		}
		for _, tok := range args[2:] {
			if tok == "on" {
				user.Enabled = true
			} else if tok == "off" {
				user.Enabled = false
			} else if strings.HasPrefix(tok, ">") {
				user.Password = strings.TrimPrefix(tok, ">")
			} else if strings.HasPrefix(tok, "~") {
				pat := strings.TrimPrefix(tok, "~")
				user.KeyPatterns = []string{pat}
			} else if tok == "admin" {
				user.IsAdmin = true
				user.ReadOnly = false
				user.Role = RoleAdmin
			} else if tok == "+@all" || tok == "+@write" || tok == "readwrite" {
				user.ReadOnly = false
				user.IsAdmin = false
				user.Role = RoleReadWrite
			} else if tok == "+@read" || tok == "readonly" {
				user.ReadOnly = true
				user.IsAdmin = false
				user.Role = RoleReadOnly
			}
		}
		e.ACL.SetUser(user)
		return resp.SimpleString("OK"), false

	case "DELUSER":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'acl deluser' command"), false
		}
		if ok := e.ACL.DeleteUser(args[1]); !ok {
			return resp.Integer(0), false
		}
		return resp.Integer(1), false

	default:
		return resp.Error(fmt.Sprintf("ERR unknown ACL subcommand '%s'", aclSub)), false
	}
}
