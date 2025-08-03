package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bharatmehan/osprey/internal/config"
	"github.com/bharatmehan/osprey/internal/protocol"
	"github.com/bharatmehan/osprey/internal/storage"
)

// Server represents the Osprey server
type Server struct {
	config   *config.Config
	store    *storage.PersistentStore
	listener net.Listener

	// Connection management
	mu          sync.RWMutex
	connections map[net.Conn]struct{}
	clientCount int32

	// Shutdown handling
	shutdown   chan struct{}
	shutdownWg sync.WaitGroup
}

// New creates a new server instance
func New(cfg *config.Config) (*Server, error) {
	store, err := storage.NewPersistentStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	return &Server{
		config:      cfg,
		store:       store,
		connections: make(map[net.Conn]struct{}),
		shutdown:    make(chan struct{}),
	}, nil
}

// Start starts the server
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return err
	}
	s.listener = listener

	// No need to start sweeper here as it's handled by PersistentStore

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return nil
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		// Check client limit
		if atomic.LoadInt32(&s.clientCount) >= int32(s.config.MaxClients) {
			conn.Close()
			continue
		}

		s.mu.Lock()
		s.connections[conn] = struct{}{}
		s.mu.Unlock()

		atomic.AddInt32(&s.clientCount, 1)

		s.shutdownWg.Add(1)
		go s.handleConnection(conn)
	}
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown() error {
	close(s.shutdown)

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	s.mu.Lock()
	for conn := range s.connections {
		conn.Close()
	}
	s.mu.Unlock()

	// Wait for all goroutines
	s.shutdownWg.Wait()

	// Close the store
	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

// GetAddress returns the actual listening address (useful for testing with auto-assigned ports)
func (s *Server) GetAddress() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.config.ListenAddr
}

// handleConnection handles a client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()

		atomic.AddInt32(&s.clientCount, -1)
		conn.Close()
		s.shutdownWg.Done()
	}()

	parser := protocol.NewParser(conn)
	writer := bufio.NewWriter(conn)

	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(time.Minute))

		cmd, err := parser.ParseCommand()
		if err != nil {
			if err == io.EOF || errors.Is(err, net.ErrClosed) {
				return
			}
			protocol.WriteError(writer, "BADREQ", err.Error())
			writer.Flush()
			continue
		}

		// Process command
		start := time.Now()
		s.processCommand(cmd, writer)
		writer.Flush()

		// Log slow commands
		duration := time.Since(start)
		if duration > s.config.SlowlogThreshold() {
			log.Printf("Slow command: %s %v took %v", cmd.Name, cmd.Args, duration)
		}
	}
}

// processCommand processes a single command
func (s *Server) processCommand(cmd *protocol.Command, w io.Writer) {
	// Check if we're in snapshot pause for mutating commands
	if s.isMutatingCommand(cmd.Name) {
		if s.store.IsSnapshotPaused() {
			protocol.WriteError(w, "BUSY", "server is busy")
			return
		}
	}

	switch cmd.Name {
	case "PING":
		s.handlePing(w)
	case "GET":
		s.handleGet(cmd, w)
	case "SET":
		s.handleSet(cmd, w)
	case "DEL":
		s.handleDel(cmd, w)
	case "EXISTS":
		s.handleExists(cmd, w)
	case "EXPIRE":
		s.handleExpire(cmd, w)
	case "TTL":
		s.handleTTL(cmd, w)
	case "INCR":
		s.handleIncr(cmd, w, 1)
	case "DECR":
		s.handleIncr(cmd, w, -1)
	case "STATS":
		s.handleStats(cmd, w)
	case "MGET":
		s.handleMGet(cmd, w)
	case "MSET":
		s.handleMSet(cmd, w)
	default:
		protocol.WriteError(w, "BADREQ", "unknown command")
	}
}

// isMutatingCommand checks if a command is mutating
func (s *Server) isMutatingCommand(cmd string) bool {
	switch cmd {
	case "SET", "DEL", "EXPIRE", "INCR", "DECR", "MSET":
		return true
	default:
		return false
	}
}
