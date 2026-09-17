package reactor

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vortexkv/vortexkv/internal/engine"
)

// FallbackServer implements a high-performance standard net.Listener server
// using the reactor's zero-allocation ring buffers and RESP serializers.
type FallbackServer struct {
	cfg       Config
	listener  net.Listener
	stopChan  chan struct{}
	wg        sync.WaitGroup
	connCount atomic.Int64
}

// NewFallbackReactor creates a standard netpoller reactor fallback.
func NewFallbackReactor(cfg Config) (*FallbackServer, error) {
	if cfg.RingSize <= 0 {
		cfg.RingSize = 64 * 1024
	}
	return &FallbackServer{
		cfg:      cfg,
		stopChan: make(chan struct{}),
	}, nil
}

func (s *FallbackServer) Start() error {
	l, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("fallback listen: %w", err)
	}
	s.listener = l
	log.Printf("[VortexKV] 🚀 High-Velocity Net Reactor listening on %s", s.cfg.Addr)

	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

func (s *FallbackServer) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopChan:
				return
			default:
				time.Sleep(1 * time.Millisecond)
				continue
			}
		}

		if s.cfg.MaxClients > 0 && s.connCount.Load() >= s.cfg.MaxClients {
			_, _ = conn.Write([]byte("-ERR max number of clients reached\r\n"))
			_ = conn.Close()
			continue
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
			_ = tcpConn.SetReadBuffer(s.cfg.RingSize)
			_ = tcpConn.SetWriteBuffer(s.cfg.RingSize)
		}

		connID := fmt.Sprintf("conn-%d", s.connCount.Add(1))
		s.cfg.Engine.Telemetry.IncrConnections()

		go s.handleConnection(connID, conn)
	}
}

func (s *FallbackServer) handleConnection(connID string, conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.connCount.Add(-1)
		s.cfg.Engine.ClearClientSession(connID)
		s.cfg.Engine.Telemetry.DecrConnections()
	}()

	session := s.cfg.Engine.GetClientSession(connID)
	inRing := NewRingBuffer(s.cfg.RingSize)
	outBuf := make([]byte, 0, 64*1024)
	readBuf := make([]byte, 32*1024)

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}
		if n == 0 {
			continue
		}

		_, _ = inRing.Write(readBuf[:n])

		for {
			data := inRing.Peek()
			if inRing.Len() > len(data) {
				data = inRing.Bytes()
			}
			if len(data) == 0 {
				break
			}

			args, consumed, err := ParseCommand(data)
			if err != nil {
				_, _ = conn.Write([]byte("-ERR protocol error\r\n"))
				return
			}
			if consumed == 0 || args == nil {
				break // need more data
			}

			inRing.AdvanceRead(consumed)

			cmdUpper := engine.ToUpperFast(args[0])
			if cmdUpper == "QUIT" {
				_, _ = conn.Write(respOK)
				return
			}

			res := s.cfg.Engine.ExecuteCommandWithSession(session, connID, args)
			outBuf = AppendValue(outBuf, res)
		}

		if len(outBuf) > 0 {
			_, err := conn.Write(outBuf)
			if err != nil {
				return
			}
			outBuf = outBuf[:0]
		}
	}
}

func (s *FallbackServer) Stop() error {
	select {
	case <-s.stopChan:
		return nil
	default:
		close(s.stopChan)
	}

	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	return nil
}
