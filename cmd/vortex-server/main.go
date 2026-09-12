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

	"github.com/vortexkv/vortexkv/internal/engine"
	"github.com/vortexkv/vortexkv/internal/persistence"
	"github.com/vortexkv/vortexkv/internal/server"
	"github.com/vortexkv/vortexkv/internal/web"
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

	webEnabled := flag.Bool("web-enabled", true, "Enable embedded Web Studio dashboard")
	webPort := flag.Int("web-port", 7380, "Port for Visual Studio Web Dashboard & WebSockets (default: 7380)")
	webBind := flag.String("web-bind", "0.0.0.0", "Network address to bind Web Studio")

	aofPath := flag.String("aof", "vortex.aof", "Path to Append-Only File (leave empty to disable persistence)")
	fsync := flag.String("fsync", "everysec", "Fsync policy for AOF: always | everysec | no")
	flag.Parse()

	fmt.Print(banner)

	fsyncPol := persistence.FsyncEverySec
	switch *fsync {
	case "always":
		fsyncPol = persistence.FsyncAlways
	case "no":
		fsyncPol = persistence.FsyncNo
	}

	eng, err := engine.NewEngine(*aofPath, fsyncPol)
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

	addr := fmt.Sprintf("%s:%d", *bind, *port)
	tcpServer := server.NewTCPServer(addr, eng)
	tcpServer.MaxClients = *maxclients

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

	if eng.MaxMemory > 0 {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 💾 MaxMemory: %s with allkeys-lru eviction\n", *maxmemoryStr)
	}

	if *webEnabled {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 🌌 Immersive Visual Studio: \033[1;32mhttp://%s:%d\033[0m\n", *webBind, *webPort)
	}
	if *aofPath != "" {
		fmt.Printf("\033[38;2;0;243;255m[VortexKV]\033[0m 💾 AOF Persistence active: %s (fsync=%s)\n", *aofPath, *fsync)
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
