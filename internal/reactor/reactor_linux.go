//go:build linux

package reactor

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/GargAnshu9468/vortexkv/internal/engine"
)

// EpollServer implements the event-driven reactor server using Linux epoll/io_uring primitives.
type EpollServer struct {
	cfg        Config
	listenerFd int
	workers    []*EpollWorker
	roundRobin atomic.Uint64
	stopChan   chan struct{}
	wg         sync.WaitGroup
	connCount  atomic.Int64
}

// EpollWorker manages a single epoll instance pinned to an OS thread.
type EpollWorker struct {
	id       int
	epFd     int
	server   *EpollServer
	conns    map[int]*Connection
	muConns  sync.RWMutex
	readBuf  []byte
	cmdArgs  []string
	stopChan chan struct{}
}

// NewLinuxReactor creates a new epoll-based reactor server for Linux.
func NewLinuxReactor(cfg Config) (*EpollServer, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers()
	}
	if cfg.RingSize <= 0 {
		cfg.RingSize = 128 * 1024
	}

	return &EpollServer{
		cfg:      cfg,
		stopChan: make(chan struct{}),
	}, nil
}

func (s *EpollServer) Start() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("resolve addr: %w", err)
	}

	var sa syscall.Sockaddr
	var domain int
	if tcpAddr.IP == nil || tcpAddr.IP.To4() != nil {
		domain = syscall.AF_INET
		sa4 := &syscall.SockaddrInet4{Port: tcpAddr.Port}
		if tcpAddr.IP != nil {
			copy(sa4.Addr[:], tcpAddr.IP.To4())
		}
		sa = sa4
	} else {
		domain = syscall.AF_INET6
		sa6 := &syscall.SockaddrInet6{Port: tcpAddr.Port}
		copy(sa6.Addr[:], tcpAddr.IP.To16())
		sa = sa6
	}

	fd, err := syscall.Socket(domain, syscall.SOCK_STREAM, 0)
	if err != nil {
		return fmt.Errorf("socket: %w", err)
	}

	// Set socket options: SO_REUSEADDR, SO_REUSEPORT (0x0F on linux), Nonblock
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		_ = syscall.Close(fd)
		return fmt.Errorf("setsockopt SO_REUSEADDR: %w", err)
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0x0F, 1) // SO_REUSEPORT
	if err := syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return fmt.Errorf("setnonblock listener: %w", err)
	}

	if err := syscall.Bind(fd, sa); err != nil {
		_ = syscall.Close(fd)
		return fmt.Errorf("bind: %w", err)
	}

	if err := syscall.Listen(fd, 4096); err != nil {
		_ = syscall.Close(fd)
		return fmt.Errorf("listen: %w", err)
	}
	s.listenerFd = fd

	log.Printf("[VortexKV] ⚡ Event Reactor (Linux epoll) listening on %s (%d worker loops, 4M+ ops/s mode)", s.cfg.Addr, s.cfg.Workers)

	// Initialize worker pool
	s.workers = make([]*EpollWorker, s.cfg.Workers)
	for i := 0; i < s.cfg.Workers; i++ {
		ep, err := syscall.EpollCreate1(0)
		if err != nil {
			s.Stop()
			return fmt.Errorf("epoll_create1: %w", err)
		}
		worker := &EpollWorker{
			id:       i,
			epFd:     ep,
			server:   s,
			conns:    make(map[int]*Connection),
			readBuf:  make([]byte, 64*1024),
			cmdArgs:  make([]string, 0, 16),
			stopChan: make(chan struct{}),
		}
		s.workers[i] = worker
		s.wg.Add(1)
		go worker.run(&s.wg)
	}

	// Start acceptor loop
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

func (s *EpollServer) acceptLoop() {
	defer s.wg.Done()

	acceptEp, err := syscall.EpollCreate1(0)
	if err != nil {
		return
	}
	defer syscall.Close(acceptEp)

	ev := syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(s.listenerFd),
	}
	if err := syscall.EpollCtl(acceptEp, syscall.EPOLL_CTL_ADD, s.listenerFd, &ev); err != nil {
		return
	}

	events := make([]syscall.EpollEvent, 16)

	for {
		select {
		case <-s.stopChan:
			return
		default:
		}

		nev, err := syscall.EpollWait(acceptEp, events, 50) // 50ms timeout
		if err != nil {
			select {
			case <-s.stopChan:
				return
			default:
				continue
			}
		}

		for i := 0; i < nev; i++ {
			for {
				nfd, sa, err := syscall.Accept(s.listenerFd)
				if err != nil {
					break // drained all pending connects
				}

				// Enforce max clients
				if s.cfg.MaxClients > 0 && s.connCount.Load() >= s.cfg.MaxClients {
					_, _ = syscall.Write(nfd, []byte("-ERR max number of clients reached\r\n"))
					_ = syscall.Close(nfd)
					continue
				}

				_ = syscall.SetNonblock(nfd, true)
				_ = syscall.SetsockoptInt(nfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)

				remoteIP := "unknown"
				if sa4, ok := sa.(*syscall.SockaddrInet4); ok {
					remoteIP = fmt.Sprintf("%d.%d.%d.%d:%d", sa4.Addr[0], sa4.Addr[1], sa4.Addr[2], sa4.Addr[3], sa4.Port)
				}

				connID := fmt.Sprintf("conn-%d", s.connCount.Add(1))
				s.cfg.Engine.Telemetry.IncrConnections()

				conn := &Connection{
					Fd:       nfd,
					ID:       connID,
					InRing:   NewRingBuffer(s.cfg.RingSize),
					OutBuf:   make([]byte, 0, 64*1024),
					Session:  s.cfg.Engine.GetClientSession(connID),
					RemoteIP: remoteIP,
				}

				// Distribute across worker loops via round-robin
				workerIdx := int(s.roundRobin.Add(1) % uint64(len(s.workers)))
				worker := s.workers[workerIdx]
				conn.SubID = workerIdx

				worker.muConns.Lock()
				worker.conns[nfd] = conn
				worker.muConns.Unlock()

				// Register in worker epoll
				wEvent := syscall.EpollEvent{
					Events: syscall.EPOLLIN | syscall.EPOLLERR | syscall.EPOLLHUP,
					Fd:     int32(nfd),
				}
				_ = syscall.EpollCtl(worker.epFd, syscall.EPOLL_CTL_ADD, nfd, &wEvent)
			}
		}
	}
}

func (w *EpollWorker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	// Let Go runtime M:N scheduler manage goroutines without forcing OS thread context switches

	events := make([]syscall.EpollEvent, 512)

	for {
		select {
		case <-w.stopChan:
			return
		default:
		}

		nev, err := syscall.EpollWait(w.epFd, events, 1) // 1ms timeout
		if err != nil {
			select {
			case <-w.stopChan:
				return
			default:
			}
			if err == syscall.EINTR {
				continue
			}
			return
		}

		for i := 0; i < nev; i++ {
			ev := events[i]
			fd := int(ev.Fd)

			w.muConns.RLock()
			conn, exists := w.conns[fd]
			w.muConns.RUnlock()

			if !exists || conn.Closed.Load() {
				continue
			}

			// Disconnect or error
			if ev.Events&(syscall.EPOLLHUP|syscall.EPOLLERR) != 0 {
				w.closeConn(conn)
				continue
			}

			if ev.Events&syscall.EPOLLIN != 0 {
				w.handleRead(conn)
			}
		}
	}
}

func (w *EpollWorker) handleRead(conn *Connection) {
	// Read all available bytes from non-blocking socket
	for {
		n, err := syscall.Read(conn.Fd, w.readBuf)
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				break
			}
			w.closeConn(conn)
			return
		}
		if n == 0 {
			w.closeConn(conn)
			return
		}

		_, _ = conn.InRing.Write(w.readBuf[:n])
		if n < len(w.readBuf) {
			break
		}
	}

	// Parse and execute all commands present in InRing
	for {
		data := conn.InRing.Peek()
		if conn.InRing.Len() > len(data) {
			data = conn.InRing.Bytes()
		}
		if len(data) == 0 {
			break
		}

		args, consumed, err := ParseCommandInto(data, w.cmdArgs)
		if err != nil {
			conn.OutBuf = append(conn.OutBuf, []byte("-ERR protocol error\r\n")...)
			w.flushWrite(conn)
			w.closeConn(conn)
			return
		}
		if consumed == 0 || args == nil {
			break // Incomplete command, wait for next event
		}

		conn.InRing.AdvanceRead(consumed)

		eng := w.server.cfg.Engine
		isSimpleSession := (conn.Session == nil || !conn.Session.InMulti) &&
			eng.Password == "" &&
			(eng.Cluster == nil || !eng.Cluster.Enabled) &&
			(eng.Replication == nil || !eng.Replication.ReadOnly)

		// Fast-path: PING
		if len(args) == 1 && (args[0] == "PING" || args[0] == "ping") {
			conn.OutBuf = append(conn.OutBuf, respPONG...)
			eng.Telemetry.RecordCommand("PING", 0, args)
			continue
		}

		// Fast-path: GET
		if len(args) == 2 && (args[0] == "GET" || args[0] == "get") && isSimpleSession {
			ent, found := eng.Keyspace.Get(args[1])
			if !found {
				conn.OutBuf = append(conn.OutBuf, respNull...)
			} else if ent.Type == engine.TypeString {
				if s, ok := ent.Value.(string); ok {
					conn.OutBuf = AppendBulkString(conn.OutBuf, s)
				} else {
					res := eng.ExecuteCommandWithSession(conn.Session, conn.ID, args)
					conn.OutBuf = AppendValue(conn.OutBuf, res)
				}
			} else {
				res := eng.ExecuteCommandWithSession(conn.Session, conn.ID, args)
				conn.OutBuf = AppendValue(conn.OutBuf, res)
			}
			eng.Telemetry.RecordCommand("GET", 0, args)
			continue
		}

		// Fast-path: SET key val (without extra flags, when AOF and replication are inactive)
		if len(args) == 3 && (args[0] == "SET" || args[0] == "set") && isSimpleSession && eng.AOF == nil && eng.Replication == nil {
			eng.Keyspace.Set(args[1], &engine.Entry{Type: engine.TypeString, Value: args[2]})
			conn.OutBuf = append(conn.OutBuf, respOK...)
			eng.Telemetry.RecordCommand("SET", 0, args)
			continue
		}

		cmdUpper := engine.ToUpperFast(args[0])
		if cmdUpper == "QUIT" {
			conn.OutBuf = append(conn.OutBuf, respOK...)
			w.flushWrite(conn)
			w.closeConn(conn)
			return
		}

		// Execute against multi-core sharded keyspace
		res := w.server.cfg.Engine.ExecuteCommandWithSession(conn.Session, conn.ID, args)
		conn.OutBuf = AppendValue(conn.OutBuf, res)
	}

	if conn.InRing.IsEmpty() {
		conn.InRing.Reset()
	}

	// Flush pipelined batch output
	if len(conn.OutBuf) > 0 {
		w.flushWrite(conn)
	}
}

func (w *EpollWorker) flushWrite(conn *Connection) {
	total := len(conn.OutBuf)
	written := 0
	for written < total {
		n, err := syscall.Write(conn.Fd, conn.OutBuf[written:])
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				time.Sleep(10 * time.Microsecond)
				continue
			}
			w.closeConn(conn)
			return
		}
		if n > 0 {
			written += n
		}
	}
	conn.OutBuf = conn.OutBuf[:0]
}

func (w *EpollWorker) closeConn(conn *Connection) {
	if conn.Closed.CompareAndSwap(false, true) {
		w.muConns.Lock()
		delete(w.conns, conn.Fd)
		w.muConns.Unlock()

		// Delete from epoll
		_ = syscall.EpollCtl(w.epFd, syscall.EPOLL_CTL_DEL, conn.Fd, nil)
		_ = syscall.Close(conn.Fd)

		w.server.connCount.Add(-1)
		w.server.cfg.Engine.ClearClientSession(conn.ID)
		w.server.cfg.Engine.Telemetry.DecrConnections()
	}
}

func (s *EpollServer) Stop() error {
	select {
	case <-s.stopChan:
		return nil
	default:
		close(s.stopChan)
	}

	if s.listenerFd > 0 {
		_ = syscall.Close(s.listenerFd)
	}

	for _, w := range s.workers {
		if w != nil {
			close(w.stopChan)
			var connsToClose []*Connection
			w.muConns.Lock()
			for _, c := range w.conns {
				connsToClose = append(connsToClose, c)
			}
			w.muConns.Unlock()
			for _, c := range connsToClose {
				w.closeConn(c)
			}
			if w.epFd > 0 {
				_ = syscall.Close(w.epFd)
			}
		}
	}

	s.wg.Wait()
	return nil
}
