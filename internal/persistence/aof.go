package persistence

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"

	"github.com/vortexkv/vortexkv/internal/resp"
)

type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverySec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

type AOF struct {
	mu         sync.Mutex
	file       *os.File
	writer     *bufio.Writer
	policy     FsyncPolicy
	stopChan   chan struct{}
	syncTicker *time.Ticker
	path       string
}

func OpenAOF(path string, policy FsyncPolicy) (*AOF, error) {
	// 0600 restricts database dump read/write access strictly to the running user
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	a := &AOF{
		file:     f,
		writer:   bufio.NewWriterSize(f, 64*1024),
		policy:   policy,
		stopChan: make(chan struct{}),
		path:     path,
	}

	if policy == FsyncEverySec {
		a.syncTicker = time.NewTicker(1 * time.Second)
		go a.fsyncLoop()
	}

	return a, nil
}

func (a *AOF) fsyncLoop() {
	for {
		select {
		case <-a.stopChan:
			return
		case <-a.syncTicker.C:
			a.mu.Lock()
			_ = a.writer.Flush()
			_ = a.file.Sync()
			a.mu.Unlock()
		}
	}
}

func (a *AOF) WriteCommand(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	bytes := resp.SerializeCommand(args)
	if _, err := a.writer.Write(bytes); err != nil {
		return err
	}

	if a.policy == FsyncAlways {
		if err := a.writer.Flush(); err != nil {
			return err
		}
		return a.file.Sync()
	}

	return nil
}

func (a *AOF) Close() error {
	if a.syncTicker != nil {
		a.syncTicker.Stop()
	}
	close(a.stopChan)

	a.mu.Lock()
	defer a.mu.Unlock()

	_ = a.writer.Flush()
	_ = a.file.Sync()
	return a.file.Close()
}

// Replay loads commands from an AOF file on startup
func Replay(path string, handler func(args []string) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	reader := resp.NewReader(f)
	for {
		cmd, err := reader.ReadCommand()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if len(cmd) > 0 {
			if err := handler(cmd); err != nil {
				// log or continue
			}
		}
	}
	return nil
}
