package web

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vortexkv/vortexkv/internal/cluster"
	"github.com/vortexkv/vortexkv/internal/datastruct"
	"github.com/vortexkv/vortexkv/internal/engine"
	"github.com/vortexkv/vortexkv/internal/resp"
)

//go:embed dist/*
var embeddedDist embed.FS

type Server struct {
	addr   string
	engine *engine.Engine
	server *http.Server
}

func NewServer(addr string, eng *engine.Engine) *Server {
	s := &Server{
		addr:   addr,
		engine: eng,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/auth/status", s.handleAuthStatus)
	mux.HandleFunc("/api/auth/login", s.handleAuthLogin)
	mux.HandleFunc("/api/auth/logout", s.handleAuthLogout)
	mux.HandleFunc("/api/auth/setup", s.handleAuthSetup)
	mux.HandleFunc("/api/acl/users", s.authMiddleware(s.handleACLListUsers))
	mux.HandleFunc("/api/acl/user", s.authMiddleware(s.handleACLSaveUser))
	mux.HandleFunc("/api/acl/user/delete", s.authMiddleware(s.handleACLDeleteUser))
	mux.HandleFunc("/api/replication", s.authMiddleware(s.handleReplicationStatus))
	mux.HandleFunc("/api/replication/promote", s.authMiddleware(s.handleReplicationPromote))
	mux.HandleFunc("/api/replication/replicate", s.authMiddleware(s.handleReplicationReplicate))
	mux.HandleFunc("/api/cluster", s.authMiddleware(s.handleClusterStatus))
	mux.HandleFunc("/api/cluster/keyslot", s.authMiddleware(s.handleClusterKeyslot))
	mux.HandleFunc("/api/info", s.authMiddleware(s.handleInfo))
	mux.HandleFunc("/api/metrics", s.authMiddleware(s.handleMetrics))
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/metrics", s.handlePrometheusMetrics)
	mux.HandleFunc("/api/keys", s.authMiddleware(s.handleKeys))
	mux.HandleFunc("/api/key", s.authMiddleware(s.handleKeyDetail))
	mux.HandleFunc("/api/streams", s.authMiddleware(s.handleStreamsList))
	mux.HandleFunc("/api/stream/messages", s.authMiddleware(s.handleStreamMessages))
	mux.HandleFunc("/api/stream/groups", s.authMiddleware(s.handleStreamGroups))
	mux.HandleFunc("/api/stream/xadd", s.authMiddleware(s.handleStreamXAdd))
	mux.HandleFunc("/api/stream/group/create", s.authMiddleware(s.handleStreamGroupCreate))
	mux.HandleFunc("/api/stream/xack", s.authMiddleware(s.handleStreamXAck))
	mux.HandleFunc("/api/exec", s.authMiddleware(s.handleExec))
	mux.HandleFunc("/api/slowlog", s.authMiddleware(s.handleSlowLog))
	mux.HandleFunc("/ws", s.handleWebSocket)

	// Static SPA serving
	distSub, err := fs.Sub(embeddedDist, "dist")
	if err == nil {
		fileServer := http.FileServer(http.FS(distSub))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api") || r.URL.Path == "/ws" || r.URL.Path == "/metrics" || r.URL.Path == "/healthz" {
				return
			}
			f, err := distSub.Open(strings.TrimPrefix(r.URL.Path, "/"))
			if err == nil {
				_ = f.Close()
				fileServer.ServeHTTP(w, r)
				return
			}
			// Fallback to index.html for SPA client routing
			indexData, err := fs.ReadFile(distSub, "index.html")
			if err == nil {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = w.Write(indexData)
				return
			}
			http.NotFound(w, r)
		})
	}

	s.server = &http.Server{
		Addr:              addr,
		Handler:           corsMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	return s
}

func (s *Server) Start() error {
	log.Printf("[VortexKV] 🌌 Immersive Visual Studio running on http://%s", s.addr)
	return s.server.ListenAndServe()
}

func (s *Server) Stop() error {
	return s.server.Close()
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) checkAuth(r *http.Request) bool {
	if s.engine.Password == "" {
		return true
	}

	target := []byte(s.engine.Password)

	// 1. Check Bearer token in Authorization header
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(token), target) == 1 {
			return true
		}
	}

	// 2. Check HTTP Session Cookie (30-day persistent session)
	if cookie, err := r.Cookie("vortex_session"); err == nil && cookie.Value != "" {
		if subtle.ConstantTimeCompare([]byte(cookie.Value), target) == 1 {
			return true
		}
	}

	// 3. Check Basic Auth
	_, pass, ok := r.BasicAuth()
	if ok && subtle.ConstantTimeCompare([]byte(pass), target) == 1 {
		return true
	}

	// 4. Check query parameter ?token=...
	queryToken := r.URL.Query().Get("token")
	if queryToken != "" && subtle.ConstantTimeCompare([]byte(queryToken), target) == 1 {
		return true
	}

	return false
}

func (s *Server) authMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.checkAuth(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error":  "Authentication required",
				"status": 401,
			})
			return
		}
		handler(w, r)
	}
}

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	required := s.engine.Password != ""
	authenticated := s.checkAuth(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"auth_required": required,
		"authenticated": authenticated,
		"first_run":     !required,
	})
}

func (s *Server) handleAuthSetup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.engine.Password != "" {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "Master password is already initialized",
		})
		return
	}

	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Password) == "" {
		http.Error(w, "Invalid password provided", http.StatusBadRequest)
		return
	}

	cleanPass := strings.TrimSpace(req.Password)
	s.engine.SetMasterPassword(cleanPass)

	// Set session cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "vortex_session",
		Value:    cleanPass,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   30 * 24 * 3600,
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Master administrator password configured successfully",
		"token":   cleanPass,
	})
}

func (s *Server) handleACLListUsers(w http.ResponseWriter, r *http.Request) {
	if s.engine.ACL == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	users := s.engine.ACL.ListUsers()
	writeJSON(w, http.StatusOK, users)
}

func (s *Server) handleACLSaveUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username    string   `json:"username"`
		Password    string   `json:"password"`
		Role        string   `json:"role"`
		KeyPattern  string   `json:"key_pattern"`
		KeyPatterns []string `json:"key_patterns"`
		Enabled     bool     `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Username) == "" {
		http.Error(w, "Invalid user payload", http.StatusBadRequest)
		return
	}

	username := strings.TrimSpace(req.Username)
	role := engine.UserRole(req.Role)
	if role == "" {
		role = engine.RoleReadWrite
	}

	var patterns []string
	if len(req.KeyPatterns) > 0 {
		for _, p := range req.KeyPatterns {
			if s := strings.TrimSpace(p); s != "" {
				patterns = append(patterns, s)
			}
		}
	}
	if len(patterns) == 0 {
		pat := strings.TrimSpace(req.KeyPattern)
		if pat == "" {
			pat = "*"
		}
		patterns = []string{pat}
	}

	user := &engine.ACLUser{
		Username:    username,
		Password:    strings.TrimSpace(req.Password),
		Enabled:     req.Enabled,
		Role:        role,
		IsAdmin:     role == engine.RoleAdmin,
		ReadOnly:    role == engine.RoleReadOnly,
		KeyPatterns: patterns,
	}

	s.engine.ACL.SetUser(user)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "user": user.Username})
}

func (s *Server) handleACLDeleteUser(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.URL.Query().Get("username"))
	if name == "" {
		name = strings.TrimSpace(r.URL.Query().Get("name"))
	}
	if name == "" && r.Body != nil {
		var req struct {
			Username string `json:"username"`
			Name     string `json:"name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		name = strings.TrimSpace(req.Username)
		if name == "" {
			name = strings.TrimSpace(req.Name)
		}
	}

	if name == "" || name == "default" {
		http.Error(w, "Cannot delete default user or missing username", http.StatusBadRequest)
		return
	}

	ok := s.engine.ACL.DeleteUser(name)
	writeJSON(w, http.StatusOK, map[string]any{"success": ok, "user": name})
}

func (s *Server) handleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	if s.engine.Replication == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"role":             "master",
			"connected_slaves": 0,
			"slaves":           []any{},
			"readonly":         false,
		})
		return
	}
	status := s.engine.Replication.GetStatus()
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleReplicationPromote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.engine.Replication == nil {
		http.Error(w, "Replication manager unavailable", http.StatusInternalServerError)
		return
	}
	s.engine.Replication.PromoteToMaster()
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Node successfully promoted to independent Master",
		"role":    "master",
	})
}

func (s *Server) handleReplicationReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Host) == "" || req.Port <= 0 {
		http.Error(w, "Invalid host or port payload", http.StatusBadRequest)
		return
	}
	if s.engine.Replication == nil {
		http.Error(w, "Replication manager unavailable", http.StatusInternalServerError)
		return
	}

	authPass := req.Password
	if authPass == "" {
		authPass = s.engine.Password
	}

	s.engine.Replication.ConnectToMaster(strings.TrimSpace(req.Host), req.Port, authPass)
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Connecting to master %s:%d", req.Host, req.Port),
		"role":    "slave",
	})
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if s.engine.Cluster == nil || !s.engine.Cluster.Enabled {
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled": false,
		})
		return
	}

	type NodeInfo struct {
		ID        string `json:"id"`
		IP        string `json:"ip"`
		Port      int    `json:"port"`
		Role      string `json:"role"`
		IsSelf    bool   `json:"is_self"`
		Slots     string `json:"slots"`
		SlotCount int    `json:"slot_count"`
		LinkState string `json:"link_state"`
	}

	var nodesList []NodeInfo
	nodesList = append(nodesList, NodeInfo{
		ID:        s.engine.Cluster.Self.ID,
		IP:        s.engine.Cluster.Self.IP,
		Port:      s.engine.Cluster.Self.Port,
		Role:      s.engine.Cluster.Self.Role,
		IsSelf:    true,
		Slots:     s.engine.Cluster.Self.SlotRangesString(),
		SlotCount: s.engine.Cluster.Self.SlotCount(),
		LinkState: s.engine.Cluster.Self.LinkState,
	})

	totalAssigned := s.engine.Cluster.Self.SlotCount()
	for _, n := range s.engine.Cluster.Nodes {
		if n.ID != s.engine.Cluster.Self.ID {
			totalAssigned += n.SlotCount()
			nodesList = append(nodesList, NodeInfo{
				ID:        n.ID,
				IP:        n.IP,
				Port:      n.Port,
				Role:      n.Role,
				IsSelf:    false,
				Slots:     n.SlotRangesString(),
				SlotCount: n.SlotCount(),
				LinkState: n.LinkState,
			})
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":        true,
		"self_id":        s.engine.Cluster.Self.ID,
		"nodes":          nodesList,
		"raw_nodes":      s.engine.Cluster.FormatNodes(),
		"raw_info":       s.engine.Cluster.FormatInfo(),
		"assigned_slots": totalAssigned,
		"total_slots":    16384,
	})
}

func (s *Server) handleClusterKeyslot(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	slot := cluster.KeySlot(key)
	tag := cluster.ExtractHashTag(key)
	crc := cluster.CRC16([]byte(tag))

	owner := "Unassigned"
	if s.engine.Cluster != nil {
		if o := s.engine.Cluster.GetSlotOwner(slot); o != nil {
			owner = fmt.Sprintf("%s:%d (%s)", o.IP, o.Port, o.ID[:8])
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"key":   key,
		"tag":   tag,
		"crc16": fmt.Sprintf("0x%04X", crc),
		"slot":  slot,
		"owner": owner,
	})
}

func (s *Server) handleAuthLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	if s.engine.Password != "" {
		if subtle.ConstantTimeCompare([]byte(req.Password), []byte(s.engine.Password)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"authenticated": false,
				"error":         "Invalid password",
			})
			return
		}
	}

	// Set secure 30-day persistent session cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "vortex_session",
		Value:    req.Password,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   30 * 24 * 3600, // 30 days
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": true,
		"token":         req.Password,
	})
}

func (s *Server) handleAuthLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "vortex_session",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": false,
		"logged_out":    true,
	})
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	totalKeys := s.engine.Keyspace.TotalKeys()
	infoStr := s.engine.Telemetry.GenerateRedisInfo(totalKeys)
	writeJSON(w, http.StatusOK, map[string]any{
		"raw_info": infoStr,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	totalKeys := s.engine.Keyspace.TotalKeys()
	snap := s.engine.Telemetry.GetSnapshot(totalKeys)
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	totalKeys := s.engine.Keyspace.TotalKeys()
	snap := s.engine.Telemetry.GetSnapshot(totalKeys)
	role := "master"
	replicas := 0
	if s.engine.Replication != nil {
		role = string(s.engine.Replication.Role)
		replicas = s.engine.Replication.GetConnectedReplicasCount()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "healthy",
		"engine":         "VortexKV",
		"version":        "1.0.0-PROD",
		"role":           role,
		"replicas":       replicas,
		"uptime_seconds": snap.UptimeSeconds,
		"keys":           snap.TotalKeys,
		"connections":    snap.ActiveConnections,
		"ops_per_sec":    snap.OpsPerSecond,
	})
}

func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	// If password is required and authorization header/token was provided, validate it
	if s.engine.Password != "" && (r.Header.Get("Authorization") != "" || r.URL.Query().Get("token") != "") {
		if !s.checkAuth(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="VortexKV Metrics"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}
	totalKeys := s.engine.Keyspace.TotalKeys()
	role := "master"
	replicas := 0
	offset := int64(0)
	if s.engine.Replication != nil {
		role = string(s.engine.Replication.Role)
		replicas = s.engine.Replication.GetConnectedReplicasCount()
		offset = s.engine.Replication.MasterOffset.Load()
	}
	data := s.engine.Telemetry.PrometheusMetrics(totalKeys, role, replicas, offset)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = "*"
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			limit = val
		}
	}

	metas := s.engine.Keyspace.ListKeyMetas(pattern, limit)
	total := s.engine.Keyspace.TotalKeys()

	writeJSON(w, http.StatusOK, map[string]any{
		"total_keys": total,
		"keys":       metas,
	})
}

func (s *Server) handleKeyDetail(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("name")
	if key == "" {
		http.Error(w, "missing name query parameter", http.StatusBadRequest)
		return
	}

	entry, exists := s.engine.Keyspace.Get(key)
	if !exists {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	var valRepresentation any
	switch entry.Type {
	case engine.TypeString:
		valRepresentation = entry.Value.(string)
	case engine.TypeHash:
		valRepresentation = entry.Value.(*datastruct.Hash).GetAll()
	case engine.TypeList:
		valRepresentation = entry.Value.(*datastruct.List).Range(0, 100)
	case engine.TypeSet:
		valRepresentation = entry.Value.(*datastruct.Set).Members()
	case engine.TypeZSet:
		valRepresentation = entry.Value.(*datastruct.SkipList).Range(0, 100, false)
	case engine.TypeStream:
		valRepresentation = entry.Value.(*datastruct.Stream).Range("-", "+", 50)
	case engine.TypeVector:
		vi := entry.Value.(*datastruct.VectorIndex)
		valRepresentation = map[string]any{
			"dimension": vi.Dimension,
			"count":     vi.Len(),
		}
	}

	ttl := s.engine.Keyspace.TTL(key)

	writeJSON(w, http.StatusOK, map[string]any{
		"key":        key,
		"type":       entry.Type,
		"ttl":        ttl,
		"updated_at": entry.UpdatedAt,
		"value":      valRepresentation,
	})
}

func (s *Server) handleStreamsList(w http.ResponseWriter, r *http.Request) {
	streams := s.engine.Keyspace.ListStreams()
	if streams == nil {
		streams = []engine.StreamMeta{}
	}
	sort.Slice(streams, func(i, j int) bool {
		return streams[i].Key < streams[j].Key
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"streams": streams,
		"total":   len(streams),
	})
}

func (s *Server) handleStreamMessages(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key parameter", http.StatusBadRequest)
		return
	}

	entry, exists := s.engine.Keyspace.Get(key)
	if !exists || entry.Type != engine.TypeStream {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	st, ok := entry.Value.(*datastruct.Stream)
	if !ok {
		http.Error(w, "invalid stream object", http.StatusInternalServerError)
		return
	}

	start := r.URL.Query().Get("start")
	if start == "" {
		start = "-"
	}
	end := r.URL.Query().Get("end")
	if end == "" {
		end = "+"
	}
	count := int64(50)
	if cStr := r.URL.Query().Get("count"); cStr != "" {
		if c, err := strconv.ParseInt(cStr, 10, 64); err == nil && c > 0 {
			count = c
		}
	}

	messages := st.Range(start, end, count)
	if messages == nil {
		messages = []datastruct.StreamEntry{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"key":      key,
		"length":   st.Len(),
		"first_id": st.FirstID(),
		"last_id":  st.LastID(),
		"messages": messages,
	})
}

type GroupDetail struct {
	Name            string           `json:"name"`
	ConsumersCount  int              `json:"consumers_count"`
	PendingCount    int              `json:"pending_count"`
	LastDeliveredID string           `json:"last_delivered_id"`
	Consumers       []map[string]any `json:"consumers"`
	PEL             []PELDetail      `json:"pel"`
}

type PELDetail struct {
	ID            string `json:"id"`
	Consumer      string `json:"consumer"`
	DeliveryTime  string `json:"delivery_time"`
	DeliveryCount int64  `json:"delivery_count"`
	IdleMs        int64  `json:"idle_ms"`
}

func (s *Server) handleStreamGroups(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key parameter", http.StatusBadRequest)
		return
	}

	entry, exists := s.engine.Keyspace.Get(key)
	if !exists || entry.Type != engine.TypeStream {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	st, ok := entry.Value.(*datastruct.Stream)
	if !ok {
		http.Error(w, "invalid stream object", http.StatusInternalServerError)
		return
	}

	groupsInfo := st.GetGroupsInfo()
	groups := make([]GroupDetail, 0, len(groupsInfo))
	now := time.Now()

	for _, g := range groupsInfo {
		gName, _ := g["name"].(string)
		consumers, _ := st.GetConsumersInfo(gName)
		if consumers == nil {
			consumers = []map[string]any{}
		}

		pelEntries, _ := st.GetPendingDetailed(gName, "-", "+", 200, "", 0)
		pel := make([]PELDetail, 0, len(pelEntries))
		for _, pe := range pelEntries {
			idle := now.Sub(pe.DeliveryTime).Milliseconds()
			if idle < 0 {
				idle = 0
			}
			pel = append(pel, PELDetail{
				ID:            pe.ID,
				Consumer:      pe.ConsumerName,
				DeliveryTime:  pe.DeliveryTime.Format(time.RFC3339),
				DeliveryCount: pe.DeliveryCount,
				IdleMs:        idle,
			})
		}

		cCount, _ := g["consumers"].(int)
		pCount, _ := g["pending"].(int)
		lastDeliv, _ := g["last-delivered-id"].(string)

		groups = append(groups, GroupDetail{
			Name:            gName,
			ConsumersCount:  cCount,
			PendingCount:    pCount,
			LastDeliveredID: lastDeliv,
			Consumers:       consumers,
			PEL:             pel,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"key":    key,
		"groups": groups,
	})
}

type StreamXAddRequest struct {
	Key    string            `json:"key"`
	ID     string            `json:"id"`
	Fields map[string]string `json:"fields"`
}

func (s *Server) handleStreamXAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req StreamXAddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request JSON", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Key) == "" {
		http.Error(w, "Stream key required", http.StatusBadRequest)
		return
	}

	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = "*"
	}

	if len(req.Fields) == 0 {
		http.Error(w, "At least one field-value pair is required", http.StatusBadRequest)
		return
	}

	args := []string{"XADD", req.Key, id}
	for k, v := range req.Fields {
		args = append(args, k, v)
	}

	res := s.engine.ExecuteCommand("", args)
	if res.Type == resp.ErrorPrefix {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": res.String(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"id":      res.String(),
		"key":     req.Key,
	})
}

type StreamGroupCreateRequest struct {
	Key      string `json:"key"`
	Group    string `json:"group"`
	ID       string `json:"id"`
	MkStream bool   `json:"mkstream"`
}

func (s *Server) handleStreamGroupCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req StreamGroupCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request JSON", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Key) == "" || strings.TrimSpace(req.Group) == "" {
		http.Error(w, "Stream key and group name required", http.StatusBadRequest)
		return
	}

	startID := strings.TrimSpace(req.ID)
	if startID == "" {
		startID = "$"
	}

	args := []string{"XGROUP", "CREATE", req.Key, req.Group, startID}
	if req.MkStream {
		args = append(args, "MKSTREAM")
	}

	res := s.engine.ExecuteCommand("", args)
	if res.Type == resp.ErrorPrefix {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": res.String(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": res.String(),
	})
}

type StreamXAckRequest struct {
	Key   string   `json:"key"`
	Group string   `json:"group"`
	IDs   []string `json:"ids"`
}

func (s *Server) handleStreamXAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req StreamXAckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request JSON", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Key) == "" || strings.TrimSpace(req.Group) == "" || len(req.IDs) == 0 {
		http.Error(w, "Stream key, group, and at least one message ID required", http.StatusBadRequest)
		return
	}

	args := append([]string{"XACK", req.Key, req.Group}, req.IDs...)
	res := s.engine.ExecuteCommand("", args)
	if res.Type == resp.ErrorPrefix {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": res.String(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"acknowledged": res.Num,
	})
}

type ExecRequest struct {
	Command string `json:"command"`
}

type ExecResponse struct {
	Command       string   `json:"command"`
	DurationMicro int64    `json:"duration_micro"`
	Result        string   `json:"result"`
	Type          string   `json:"type"`
	ArrayResult   []string `json:"array_result,omitempty"`
	IsError       bool     `json:"is_error"`
}

func (s *Server) handleExec(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	cmdStr := strings.TrimSpace(req.Command)
	if cmdStr == "" {
		http.Error(w, "Empty command", http.StatusBadRequest)
		return
	}

	args := splitCommandLine(cmdStr)
	if len(args) == 0 {
		http.Error(w, "Empty command", http.StatusBadRequest)
		return
	}

	start := time.Now()
	res := s.engine.ExecuteCommand("", args)
	duration := time.Since(start).Microseconds()

	respObj := ExecResponse{
		Command:       cmdStr,
		DurationMicro: duration,
		Result:        res.String(),
		Type:          string(res.Type),
	}

	if res.Type == '-' {
		respObj.IsError = true
	}

	if res.Type == '*' && !res.Null {
		respObj.ArrayResult = formatRESPTree(res, 0)
	}

	writeJSON(w, http.StatusOK, respObj)
}

func formatRESPTree(v resp.Value, indent int) []string {
	pad := strings.Repeat("  ", indent)
	if v.Type == resp.ArrayPrefix {
		if v.Null {
			return []string{pad + "(nil)"}
		}
		if len(v.Array) == 0 {
			return []string{pad + "(empty array)"}
		}
		var lines []string
		for idx, it := range v.Array {
			subLines := formatRESPTree(it, indent+1)
			if len(subLines) == 0 {
				continue
			}
			lines = append(lines, fmt.Sprintf("%s%d) %s", pad, idx+1, strings.TrimLeft(subLines[0], " ")))
			if len(subLines) > 1 {
				lines = append(lines, subLines[1:]...)
			}
		}
		return lines
	}
	if v.Null {
		return []string{pad + "(nil)"}
	}
	if v.Type == resp.IntegerPrefix {
		return []string{fmt.Sprintf("%s(integer) %d", pad, v.Num)}
	}
	if v.Type == resp.BulkStringPrefix {
		return []string{fmt.Sprintf("%s\"%s\"", pad, string(v.Bulk))}
	}
	return []string{pad + v.String()}
}

func (s *Server) handleSlowLog(w http.ResponseWriter, r *http.Request) {
	snap := s.engine.Telemetry.GetSnapshot(0)
	writeJSON(w, http.StatusOK, snap.RecentSlowLogs)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !s.checkAuth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	ws, err := UpgradeWebSocket(w, r)
	if err != nil {
		log.Printf("[VortexKV] WebSocket upgrade error: %v", err)
		return
	}
	defer ws.Close()

	// Subscribe to metrics and events
	eventChan := s.engine.Telemetry.SubscribeEvents()
	snapChan := s.engine.Telemetry.SubscribeSnapshots()
	defer func() {
		s.engine.Telemetry.UnsubscribeEvents(eventChan)
		s.engine.Telemetry.UnsubscribeSnapshots(snapChan)
	}()

	// Read loop to detect client disconnect
	disconnect := make(chan struct{})
	go func() {
		for {
			_, err := ws.ReadMessage()
			if err != nil {
				close(disconnect)
				return
			}
		}
	}()

	// Send initial snapshot immediately
	totalKeys := s.engine.Keyspace.TotalKeys()
	initialSnap := s.engine.Telemetry.GetSnapshot(totalKeys)
	initialData, _ := json.Marshal(map[string]any{
		"type": "metrics",
		"data": initialSnap,
	})
	_ = ws.WriteText(initialData)

	for {
		select {
		case <-disconnect:
			return
		case snap, ok := <-snapChan:
			if !ok {
				return
			}
			snap.TotalKeys = s.engine.Keyspace.TotalKeys()
			data, err := json.Marshal(map[string]any{
				"type": "metrics",
				"data": snap,
			})
			if err == nil {
				if err := ws.WriteText(data); err != nil {
					return
				}
			}
		case event, ok := <-eventChan:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err == nil {
				if err := ws.WriteText(data); err != nil {
					return
				}
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func splitCommandLine(cmd string) []string {
	var args []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		if inQuotes {
			if c == quoteChar {
				inQuotes = false
			} else if c == '\\' && i+1 < len(cmd) {
				i++
				current.WriteByte(cmd[i])
			} else {
				current.WriteByte(c)
			}
		} else {
			if c == '"' || c == '\'' {
				inQuotes = true
				quoteChar = c
			} else if c == ' ' || c == '\t' {
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
			} else {
				current.WriteByte(c)
			}
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args
}
