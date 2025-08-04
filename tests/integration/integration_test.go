package integration

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bharatmehan/osprey/internal/config"
	"github.com/bharatmehan/osprey/internal/server"
	"github.com/bharatmehan/osprey/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_BasicOperations(t *testing.T) {
	// Setup test server
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect client
	c, err := client.New(srv.Address)
	require.NoError(t, err)
	defer c.Close()

	// Test PING
	err = c.Ping()
	require.NoError(t, err)

	// Test SET/GET
	resp, err := c.Set("test_key", []byte("test_value"))
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(1), resp.Version)

	resp, err = c.Get("test_key")
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, []byte("test_value"), resp.Value)
	assert.Equal(t, uint64(1), resp.Version)

	// Test DELETE
	resp, err = c.Del("test_key")
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Key should not exist anymore
	resp, err = c.Get("test_key")
	require.NoError(t, err)
	assert.False(t, resp.Success)
}

func TestIntegration_SetOptions(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	c, err := client.New(srv.Address)
	require.NoError(t, err)
	defer c.Close()

	// Test NX option
	resp, err := c.Set("nx_key", []byte("value1"), "NX")
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Should fail because key exists
	resp, err = c.Set("nx_key", []byte("value2"), "NX")
	require.NoError(t, err)
	assert.False(t, resp.Success)

	// Test XX option
	resp, err = c.Set("nx_key", []byte("value3"), "XX")
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Should fail because key doesn't exist
	resp, err = c.Set("nonexistent", []byte("value"), "XX")
	require.NoError(t, err)
	assert.False(t, resp.Success)

	// Test version-based update
	resp, err = c.Get("nx_key")
	require.NoError(t, err)
	currentVersion := resp.Version

	resp, err = c.Set("nx_key", []byte("value4"), "VER", fmt.Sprintf("%d", currentVersion))
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Should fail with wrong version
	resp, err = c.Set("nx_key", []byte("value5"), "VER", "999")
	require.NoError(t, err)
	assert.False(t, resp.Success)
}

func TestIntegration_TTL(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	c, err := client.New(srv.Address)
	require.NoError(t, err)
	defer c.Close()

	// Set key with TTL
	resp, err := c.Set("ttl_key", []byte("ttl_value"), "EX", "1000")
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Check TTL
	resp, err = c.TTL("ttl_key")
	require.NoError(t, err)
	assert.True(t, resp.TTL > 0 && resp.TTL <= 1000)

	// Set expiry on existing key
	resp, err = c.Expire("ttl_key", 2000)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// TTL should be updated
	resp, err = c.TTL("ttl_key")
	require.NoError(t, err)
	assert.True(t, resp.TTL > 1000)

	// Non-existent key
	resp, err = c.TTL("nonexistent")
	require.NoError(t, err)
	assert.Equal(t, int64(-2), resp.TTL)
}

func TestIntegration_IncrDecr(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	c, err := client.New(srv.Address)
	require.NoError(t, err)
	defer c.Close()

	// Increment non-existent key
	resp, err := c.Incr("counter")
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, int64(1), resp.Integer)

	// Increment with delta
	resp, err = c.Incr("counter", 5)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, int64(6), resp.Integer)

	// Decrement
	resp, err = c.Decr("counter", 2)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, int64(4), resp.Integer)

	// Try to increment non-numeric value
	c.Set("text", []byte("hello"))
	resp, err = c.Incr("text")
	require.NoError(t, err)
	assert.False(t, resp.Success)
}

func TestIntegration_MultiKey(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	c, err := client.New(srv.Address)
	require.NoError(t, err)
	defer c.Close()

	// Set multiple keys individually
	c.Set("key1", []byte("value1"))
	c.Set("key2", []byte("value2"))

	// Test MGET
	responses, err := c.MGet("key1", "key2", "nonexistent")
	require.NoError(t, err)
	require.Len(t, responses, 3)

	assert.True(t, responses[0].Success)
	assert.Equal(t, []byte("value1"), responses[0].Value)

	assert.True(t, responses[1].Success)
	assert.Equal(t, []byte("value2"), responses[1].Value)

	assert.False(t, responses[2].Success)
}

func TestIntegration_Stats(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	c, err := client.New(srv.Address)
	require.NoError(t, err)
	defer c.Close()

	// Perform some operations
	c.Set("key1", []byte("value1"))
	c.Set("key2", []byte("value2"))
	c.Get("key1")
	c.Del("key2")
	c.Incr("counter")

	// Get stats
	stats, err := c.Stats()
	require.NoError(t, err)

	// Verify some expected stats
	assert.Contains(t, stats, "uptime_ms")
	assert.Contains(t, stats, "keys")
	assert.Contains(t, stats, "cmd_get")
	assert.Contains(t, stats, "cmd_set")
	assert.Contains(t, stats, "cmd_del")
	assert.Contains(t, stats, "clients")

	// Should have recorded operations
	assert.NotEqual(t, "0", stats["cmd_set"])
	assert.NotEqual(t, "0", stats["cmd_get"])
}

func TestIntegration_Persistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-integration")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Start server with persistence
	cfg := config.DefaultConfig()
	cfg.DataDir = tempDir
	cfg.ListenAddr = "localhost:0" // Auto-assign port

	srv, err := server.New(cfg)
	require.NoError(t, err)

	// Start server in background
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Start()
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Get the actual address
	address := srv.GetAddress()

	// Connect and write data
	c, err := client.New(address)
	require.NoError(t, err)

	c.Set("persistent_key1", []byte("persistent_value1"))
	c.Set("persistent_key2", []byte("persistent_value2"))
	c.Set("persistent_key3", []byte("persistent_value3"))

	c.Close()

	// Shutdown server
	srv.Shutdown()
	<-serverDone

	// Restart server with same data directory
	srv2, err := server.New(cfg)
	require.NoError(t, err)

	go func() {
		srv2.Start()
	}()

	time.Sleep(100 * time.Millisecond)

	// Connect and verify data persisted
	c2, err := client.New(srv2.GetAddress())
	require.NoError(t, err)
	defer c2.Close()

	resp, err := c2.Get("persistent_key1")
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, []byte("persistent_value1"), resp.Value)

	resp, err = c2.Get("persistent_key2")
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, []byte("persistent_value2"), resp.Value)

	srv2.Shutdown()
}

func TestIntegration_ExpiryLazy(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	c, err := client.New(srv.Address)
	require.NoError(t, err)
	defer c.Close()

	// Set key with very short expiry
	resp, err := c.Set("expiring_key", []byte("expiring_value"), "EX", "50")
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Key should exist initially
	resp, err = c.Exists("expiring_key")
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// Key should be expired (lazy deletion)
	resp, err = c.Get("expiring_key")
	require.NoError(t, err)
	assert.False(t, resp.Success)

	resp, err = c.Exists("expiring_key")
	require.NoError(t, err)
	assert.False(t, resp.Success)
}

// Test helper to setup a test server
type TestServer struct {
	Server  *server.Server
	Address string
	DataDir string
}

func setupTestServer(t *testing.T) (*TestServer, func()) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)

	cfg := config.DefaultConfig()
	cfg.DataDir = tempDir
	cfg.ListenAddr = "localhost:0" // Auto-assign port
	cfg.SweepIntervalMs = 50       // Faster sweeping for tests

	srv, err := server.New(cfg)
	require.NoError(t, err)

	// Start server in background
	go func() {
		srv.Start()
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	address := srv.GetAddress()

	testSrv := &TestServer{
		Server:  srv,
		Address: address,
		DataDir: tempDir,
	}

	cleanup := func() {
		srv.Shutdown()
		os.RemoveAll(tempDir)
	}

	return testSrv, cleanup
}

// Helper to add GetAddress method to server for testing
func init() {
	// This is a hack for testing - in real usage we know the address from config
}
