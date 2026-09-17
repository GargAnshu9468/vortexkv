package reactor

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"

	"github.com/GargAnshu9468/vortexkv/internal/engine"
)

// Server defines the interface for high-performance event reactor servers.
type Server interface {
	Start() error
	Stop() error
}

// Config configures the reactor server.
type Config struct {
	Addr       string
	Engine     *engine.Engine
	Workers    int
	RingSize   int
	MaxClients int64
	EngineType string // "auto", "reactor", "std"
}

// Connection represents an active client connection managed by a SubReactor.
type Connection struct {
	Fd            int
	ID            string
	InRing        *RingBuffer
	OutBuf        []byte
	Session       *engine.ClientSession
	SubID         int
	Closed        atomic.Bool
	RemoteIP      string
	ListeningPort int
}

// HijackToNetConn converts a raw socket fd into a net.Conn and closes the original fd.
func HijackToNetConn(fd int) (net.Conn, error) {
	file := os.NewFile(uintptr(fd), "reactor-hijack")
	defer file.Close()
	c, err := net.FileConn(file)
	if err != nil {
		return nil, err
	}
	_ = syscall.Close(fd)
	return c, nil
}

// ParseAddr splits host and port into components.
func ParseAddr(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	if host == "" {
		host = "0.0.0.0"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port: %w", err)
	}
	return host, port, nil
}

// DefaultWorkers returns the recommended worker count based on CPU cores.
func DefaultWorkers() int {
	w := runtime.GOMAXPROCS(0)
	if w < 2 {
		w = 2
	}
	return w
}
