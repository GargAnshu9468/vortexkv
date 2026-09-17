package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/GargAnshu9468/vortexkv/internal/cluster"
	"github.com/GargAnshu9468/vortexkv/internal/engine"
	"github.com/GargAnshu9468/vortexkv/internal/persistence"
	"github.com/GargAnshu9468/vortexkv/internal/replication"
	"github.com/GargAnshu9468/vortexkv/internal/server"
	"github.com/GargAnshu9468/vortexkv/internal/web"
)

const banner = `
\033[38;2;0;243;255m  ██╗   ██╗ ██████╗ ██████╗ ████████╗███████╗██╗  ██╗    ██╗  ██╗██╗   ██╗
  ██║   ██║██╔═══██╗██╔══██╗╚══██╔══╝██╔════╝╚██╗██╔╝    ██║ ██╔╝██║   ██║
  ██║   ██║██║   ██║██████╔╝   ██║   █████╗   ╚███╔╝     █████╔╝ ██║   ██║
  ╚██╗ ██╔╝██║   ██║██╔══██╗   ██║   ██╔══╝   ██╔██╗     ██╔═██╗ ╚██╗ ██╔╝
   ╚████╔╝ ╚██████╔╝██║  ██║   ██║   ███████╗██╔╝ ██╗    ██║  ██╗ ╚████╔╝ 
    ╚═══╝   ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝    ╚═╝  ╚═╝  ╚═══╝  \033[0m
  \033[38;2;138;43;226m» Next-Generation Hyper-Performance In-Memory Data Store & Studio «\033[0m
  \033[38;2;57;255;20mVersion: 1.0.0-PROD  |  Protocol: RESP2/RESP3  |  Engine: Sharded Lock-Striped\033[0m
`

func main() {
	port := flag.Int("port", 7379, "Port for VortexKV RESP wire protocol listener (default: 7379)")
	bind := flag.String("bind", "0.0.0.0", "Network address to bind VortexKV listener")
	requirepass := flag.String("requirepass", "", "Password authentication for clients and Web Studio")
	maxmemoryStr := flag.String("maxmemory", "0", "Max memory limit (e.g. 512mb, 2gb, 0 for unlimited)")
	maxclients := flag.Int64("maxclients", 10000, "Maximum concurrent client connections")
	tlsCert := flag.String("tls-cert", "", "Path to TLS certificate file (enables TLS on wire port)")
	tlsKey := flag.String("tls-key", "", "Path to TLS private key file")

	replicaof := flag.String("replicaof", "", "Master address to replicate from (e.g. '127.0.0.1:7379' or '127.0.0.1 7379')")
	masterauth := flag.String("masterauth", "", "Password to authenticate with master node")
	replicaReadOnly := flag.Bool("replica-read-only", true, "Enforce read-only access on replica node")

	clusterEnabled := flag.Bool("cluster-enabled", false, "Enable Redis cluster distributed multi-node mode")
	clusterConfigFile := flag.String("cluster-config-file", "nodes.conf", "Cluster node configuration file")
	clusterAnnounceIP := flag.String("cluster-announce-ip", "127.0.0.1", "Cluster announce IP for cluster-aware clients")
	clusterAnnouncePort := flag.Int("cluster-announce-port", 0, "Cluster announce port (default: matches -port)")
	clusterAnnounceBusPort := flag.Int("cluster-announce-bus-port", 0, "Cluster announce bus port (default: port + 10000)")

	webEnabled := flag.Bool("web-enabled", true, "Enable embedded Web Studio dashboard")
	webPort := flag.Int("web-port", 7380, "Port for Visual Studio Web Dashboard & WebSockets (default: 7380)")
	webBind := flag.String("web-bind", "0.0.0.0", "Network address to bind Web Studio")

	aofPath := flag.String("aof", "vortex.aof", "Path to Append-Only File (leave empty to disable persistence)")
	rdbPath := flag.String("rdb", "dump.rdb", "Path to binary RDB snapshot file (leave empty to disable)")
	fsync := flag.String("fsync", "everysec", "Fsync policy for AOF: always | everysec | no")

	eventEngine := flag.String("event-engine", "auto", "Network event engine: auto | reactor | std (default: auto)")
	eventWorkers := flag.Int("event-workers", 0, "Number of dedicated reactor worker loops (default: CPU cores)")
	eventRingSize := flag.Int("event-ring-size", 256*1024, "Size of reactor connection ring buffers in bytes (default: 256KB)")
	flag.Parse()

	fmt.Print(banner)

	fsyncPol := persistence.FsyncEverySec
	switch *fsync {
	case "always":
		fsyncPol = persistence.FsyncAlways
	case "no":
		fsyncPol = persistence.FsyncNo
	}

	eng, err := engine.NewEngine(*aofPath, fsyncPol, *rdbPath)
	if err != nil {
		log.Fatalf("[VortexKV] Failed to initialize engine: %v", err)
	}

	// Configure enterprise security & memory limits
	masterPass := *requirepass
	if masterPass == "" {
		if envPass := os.Getenv("VORTEX_REQUIREPASS"); envPass != "" {
			masterPass = envPass
		} else if envPass := os.Getenv("REDIS_PASSWORD"); envPass != "" {
			masterPass = envPass
		}
	}
	eng.SetMasterPassword(masterPass)
	eng.MaxMemory = parseMemory(*maxmemoryStr)

	if *clusterEnabled {
		annPort := *port
		if *clusterAnnouncePort > 0 {
			annPort = *clusterAnnouncePort
		}
		annBusPort := annPort + 10000
		if *clusterAnnounceBusPort > 0 {
			annBusPort = *clusterAnnounceBusPort
		}
		eng.Cluster = cluster.NewClusterManager(*clusterAnnounceIP, annPort, annBusPort, *clusterConfigFile)
		eng.Cluster.AuthPass = masterPass
		if err := eng.Cluster.StartBus(); err != nil {
			log.Printf("[VortexKV] Warning: cluster bus failed to start on port %d: %v", annBusPort, err)
		}
		if eng.Replication != nil {
			eng.Cluster.OnPromote = func() {
				eng.Replication.PromoteToMaster()
			}
		}
	}

	if eng.Replication != nil {
		eng.Replication.ListeningPort = *port

		if *replicaof != "" {
			eng.Replication.ReadOnly = *replicaReadOnly
			parts := strings.Fields(strings.ReplaceAll(*replicaof, ":", " "))
			if len(parts) >= 2 {
				mHost := parts[0]
				mPort, err := strconv.Atoi(parts[1])
				if err == nil {
					auth := *masterauth
					if auth == "" {
						auth = os.Getenv("VORTEX_MASTERAUTH")
					}
					eng.Replication.ConnectToMaster(mHost, mPort, auth)
				}
			}
		} else {
			eng.Replication.ReadOnly = false
		}
	}

	addr := fmt.Sprintf("%s:%d", *bind, *port)
	tcpServer := server.NewTCPServer(addr, eng)
	tcpServer.MaxClients = *maxclients
	tcpServer.EngineType = *eventEngine
	tcpServer.Workers = *eventWorkers
	tcpServer.RingSize = *eventRingSize

	if *tlsCert != "" && *tlsKey != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("[VortexKV] Failed to load TLS cert/key: %v", err)
		}
		tcpServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	if err := tcpServer.Start(); err != nil {
		log.Fatalf("[VortexKV] Failed to start TCP server: %v", err)
	}

	var webServer *web.Server
	if *webEnabled {
		webAddr := fmt.Sprintf("%s:%d", *webBind, *webPort)
		webServer = web.NewServer(webAddr, eng)
		go func() {
			if err := webServer.Start(); err != nil {
				log.Printf("[VortexKV] Web server error: %v", err)
			}
		}()
	}

	fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m ⚡ Redis Client Port: \033[1;37m%s\033[0m (redis-cli -h %s -p %d)\n", addr, *bind, *port)
	if *requirepass != "" {
		fmt.Printf("\033[38;2;57;255;20m[VortexKV]\033[0m 🔒 Security: Password authentication (requirepass) ACTIVE\n")
	} else {
		fmt.Printf("\033[38;2;255;170;0m[VortexKV]\033[0m ⚠️ Security: Running without password (use -requirepass in production)\n")
	}

	if eng.Replication != nil && eng.Replication.Role == replication.RoleReplica {
		fmt.Printf("\033[38;2;255;170;0m[VortexKV]\033[0m 🔁 Cluster Role: REPLICA of %s:%d (read-only: %v)\n", eng.Replication.MasterHost, eng.Replication.MasterPort, eng.Replication.ReadOnly)
	} else if eng.Replication != nil {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 👑 Cluster Role: MASTER node (Replication ID: %s)\n", eng.Replication.MasterReplID[:8])
	}

	if eng.Cluster != nil && eng.Cluster.Enabled {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 🌐 Distributed Cluster Mode: ACTIVE (Node ID: %s, Slots: %d/16384)\n", eng.Cluster.Self.ID[:8], eng.Cluster.Self.SlotCount())
	}

	if eng.MaxMemory > 0 {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 💾 MaxMemory: %s with allkeys-lru eviction\n", *maxmemoryStr)
	}

	if *webEnabled {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 🌌 Immersive Visual Studio: \033[1;32mhttp://%s:%d\033[0m\n", *webBind, *webPort)
	}
	if *aofPath != "" {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 💾 AOF Persistence active: %s (fsync=%s)\n", *aofPath, *fsync)
	}
	if *rdbPath != "" {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 💾 RDB Snapshots active: %s (CRC64 checksum enabled)\n", *rdbPath)
	}
	fmt.Println("\033[90m----------------------------------------------------------------------\033[0m")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[VortexKV] Shutting down gracefully...")
	_ = tcpServer.Stop()
	if webServer != nil {
		_ = webServer.Stop()
	}
	_ = eng.Close()
	log.Println("[VortexKV] Server terminated cleanly. Goodbye!")
}

func parseMemory(s string) uint64 {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" || s == "0" {
		return 0
	}
	multiplier := uint64(1)
	if strings.HasSuffix(s, "gb") || strings.HasSuffix(s, "g") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "gb"), "g")
	} else if strings.HasSuffix(s, "mb") || strings.HasSuffix(s, "m") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "mb"), "m")
	} else if strings.HasSuffix(s, "kb") || strings.HasSuffix(s, "k") {
		multiplier = 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "kb"), "k")
	}
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return val * multiplier
}
