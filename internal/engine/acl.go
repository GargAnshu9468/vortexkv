package engine

import (
	"crypto/subtle"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

type UserRole string

const (
	RoleAdmin     UserRole = "admin"
	RoleReadWrite UserRole = "readwrite"
	RoleReadOnly  UserRole = "readonly"
)

type ACLUser struct {
	Username    string   `json:"username"`
	Password    string   `json:"password,omitempty"`
	Enabled     bool     `json:"enabled"`
	IsAdmin     bool     `json:"is_admin"`
	Role        UserRole `json:"role"`
	KeyPatterns []string `json:"key_patterns"` // e.g. ["*"] or ["cache:*"]
	ReadOnly    bool     `json:"read_only"`
}

type ACLManager struct {
	mu    sync.RWMutex
	users map[string]*ACLUser
}

func NewACLManager(defaultPassword string) *ACLManager {
	mgr := &ACLManager{
		users: make(map[string]*ACLUser),
	}

	// Create default user
	mgr.users["default"] = &ACLUser{
		Username:    "default",
		Password:    defaultPassword,
		Enabled:     true,
		IsAdmin:     true,
		Role:        RoleAdmin,
		KeyPatterns: []string{"*"},
		ReadOnly:    false,
	}

	return mgr
}

func (m *ACLManager) Authenticate(username, password string) (*ACLUser, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if username == "" {
		username = "default"
	}

	user, exists := m.users[username]
	if !exists || !user.Enabled {
		return nil, false
	}

	// If no password set on account, access is granted only if password is empty
	if user.Password == "" {
		if password == "" {
			return user, true
		}
		return nil, false
	}

	if subtle.ConstantTimeCompare([]byte(password), []byte(user.Password)) == 1 {
		return user, true
	}

	return nil, false
}

func (m *ACLManager) SetUser(user *ACLUser) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(user.KeyPatterns) == 0 {
		user.KeyPatterns = []string{"*"}
	}
	m.users[user.Username] = user
}

func (m *ACLManager) GetUser(username string) (*ACLUser, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, exists := m.users[username]
	if !exists {
		return nil, false
	}
	// return safe copy
	cpy := *user
	return &cpy, true
}

func (m *ACLManager) DeleteUser(username string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if username == "default" {
		return false // Cannot delete default root user
	}

	if _, exists := m.users[username]; !exists {
		return false
	}
	delete(m.users, username)
	return true
}

func (m *ACLManager) ListUsers() []*ACLUser {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]*ACLUser, 0, len(m.users))
	for _, u := range m.users {
		cpy := *u
		cpy.Password = "" // redact password in API listings
		list = append(list, &cpy)
	}
	return list
}

func (m *ACLManager) ListUsernames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.users))
	for name := range m.users {
		names = append(names, name)
	}
	return names
}

// CanExecute evaluates whether a user has permissions to run the given command against the target keys
func (m *ACLManager) CanExecute(username, cmdName string, keys []string) (bool, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if username == "" {
		username = "default"
	}

	user, exists := m.users[username]
	if !exists || !user.Enabled {
		return false, "account is disabled or does not exist"
	}

	cmdUpper := strings.ToUpper(cmdName)

	if !user.IsAdmin {
		// Disallow administrative commands for non-admin
		switch cmdUpper {
		case "FLUSHDB", "FLUSHALL", "CONFIG", "SHUTDOWN", "BGREWRITEAOF":
			return false, fmt.Sprintf("administrative command '%s' not allowed for user '%s'", cmdName, username)
		}

		// Check read-only constraint
		if user.ReadOnly {
			switch cmdUpper {
			case "SET", "SETEX", "SETNX", "MSET", "DEL", "INCR", "INCRBY", "DECR", "DECRBY",
				"LPUSH", "RPUSH", "LPOP", "RPOP", "HSET", "HMSET", "HDEL", "HINCRBY",
				"SADD", "SREM", "ZADD", "ZREM", "ZINCRBY", "VADD", "VDEL", "XADD", "XDEL", "XTRIM", "XGROUP", "XACK", "EXPIRE", "EXPIREAT", "SAVE", "BGSAVE":
				return false, fmt.Sprintf("mutating command '%s' not allowed for read-only user '%s'", cmdName, username)
			}
		}
	}

	// Check key pattern constraints
	for _, key := range keys {
		if !m.matchesAnyPattern(key, user.KeyPatterns) {
			return false, fmt.Sprintf("key '%s' violates allowed namespace pattern(s) for user '%s'", key, username)
		}
	}

	return true, ""
}

func (m *ACLManager) matchesAnyPattern(key string, patterns []string) bool {
	for _, p := range patterns {
		if p == "*" || p == "~*" {
			return true
		}
		clean := strings.TrimPrefix(p, "~")
		if matched, _ := filepath.Match(clean, key); matched {
			return true
		}
		if strings.HasSuffix(clean, "*") && strings.HasPrefix(key, strings.TrimSuffix(clean, "*")) {
			return true
		}
	}
	return false
}
