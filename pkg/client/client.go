package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Client represents an Osprey client
type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

// Response represents a server response
type Response struct {
	Type    string
	Value   []byte
	Version uint64
	ExpiryMs int64
	TTL     int64
	Integer int64
	Error   string
	Success bool
}

// New creates a new client connection
func New(address string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, err
	}
	
	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

// Close closes the client connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// Ping sends a PING command
func (c *Client) Ping() error {
	if err := c.sendCommand("PING"); err != nil {
		return err
	}
	
	resp, err := c.readResponse()
	if err != nil {
		return err
	}
	
	if resp.Type != "PONG" {
		return fmt.Errorf("unexpected response: %s", resp.Type)
	}
	
	return nil
}

// Get retrieves a value by key
func (c *Client) Get(key string) (*Response, error) {
	if err := c.sendCommand("GET", key); err != nil {
		return nil, err
	}
	
	return c.readResponse()
}

// Set stores a key-value pair
func (c *Client) Set(key string, value []byte, options ...string) (*Response, error) {
	args := []string{"SET", key, strconv.Itoa(len(value))}
	args = append(args, options...)
	
	if err := c.sendCommandWithPayload(args, value); err != nil {
		return nil, err
	}
	
	return c.readResponse()
}

// Del deletes a key
func (c *Client) Del(key string) (*Response, error) {
	if err := c.sendCommand("DEL", key); err != nil {
		return nil, err
	}
	
	return c.readResponse()
}

// Exists checks if a key exists
func (c *Client) Exists(key string) (*Response, error) {
	if err := c.sendCommand("EXISTS", key); err != nil {
		return nil, err
	}
	
	return c.readResponse()
}

// Expire sets a TTL on a key
func (c *Client) Expire(key string, ttlMs int64) (*Response, error) {
	if err := c.sendCommand("EXPIRE", key, strconv.FormatInt(ttlMs, 10)); err != nil {
		return nil, err
	}
	
	return c.readResponse()
}

// TTL gets the TTL of a key
func (c *Client) TTL(key string) (*Response, error) {
	if err := c.sendCommand("TTL", key); err != nil {
		return nil, err
	}
	
	return c.readResponse()
}

// Incr increments a numeric value
func (c *Client) Incr(key string, delta ...int64) (*Response, error) {
	args := []string{"INCR", key}
	if len(delta) > 0 {
		args = append(args, strconv.FormatInt(delta[0], 10))
	}
	
	if err := c.sendCommand(args...); err != nil {
		return nil, err
	}
	
	return c.readResponse()
}

// Decr decrements a numeric value
func (c *Client) Decr(key string, delta ...int64) (*Response, error) {
	args := []string{"DECR", key}
	if len(delta) > 0 {
		args = append(args, strconv.FormatInt(delta[0], 10))
	}
	
	if err := c.sendCommand(args...); err != nil {
		return nil, err
	}
	
	return c.readResponse()
}

// MGet gets multiple keys
func (c *Client) MGet(keys ...string) ([]*Response, error) {
	args := append([]string{"MGET"}, keys...)
	
	if err := c.sendCommand(args...); err != nil {
		return nil, err
	}
	
	var responses []*Response
	for range keys {
		resp, err := c.readResponse()
		if err != nil {
			return nil, err
		}
		responses = append(responses, resp)
	}
	
	return responses, nil
}

// Stats gets server statistics
func (c *Client) Stats() (map[string]string, error) {
	if err := c.sendCommand("STATS"); err != nil {
		return nil, err
	}
	
	stats := make(map[string]string)
	
	for {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")
		
		if line == "END" {
			break
		}
		
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			stats[parts[0]] = parts[1]
		}
	}
	
	return stats, nil
}

// sendCommand sends a command without payload
func (c *Client) sendCommand(args ...string) error {
	command := strings.Join(args, " ") + "\r\n"
	_, err := c.writer.WriteString(command)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// sendCommandWithPayload sends a command with binary payload
func (c *Client) sendCommandWithPayload(args []string, payload []byte) error {
	command := strings.Join(args, " ") + "\r\n"
	_, err := c.writer.WriteString(command)
	if err != nil {
		return err
	}
	
	_, err = c.writer.Write(payload)
	if err != nil {
		return err
	}
	
	_, err = c.writer.WriteString("\r\n")
	if err != nil {
		return err
	}
	
	return c.writer.Flush()
}

// readResponse reads and parses a server response
func (c *Client) readResponse() (*Response, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty response")
	}
	
	resp := &Response{Type: parts[0]}
	
	switch parts[0] {
	case "OK":
		resp.Success = true
		if len(parts) > 1 {
			resp.Version, _ = strconv.ParseUint(parts[1], 10, 64)
		}
		
	case "PONG":
		resp.Success = true
		
	case "NOT_FOUND":
		resp.Success = false
		
	case "VALUE":
		if len(parts) < 4 {
			return nil, fmt.Errorf("invalid VALUE response")
		}
		
		length, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid length in VALUE response")
		}
		
		resp.Version, _ = strconv.ParseUint(parts[2], 10, 64)
		resp.ExpiryMs, _ = strconv.ParseInt(parts[3], 10, 64)
		
		// Read the value
		value := make([]byte, length)
		_, err = io.ReadFull(c.reader, value)
		if err != nil {
			return nil, err
		}
		
		// Read trailing \r\n
		c.reader.ReadString('\n')
		
		resp.Value = value
		resp.Success = true
		
	case "DELETED":
		if len(parts) > 1 {
			deleted, _ := strconv.Atoi(parts[1])
			resp.Success = deleted == 1
		}
		
	case "EXISTS":
		if len(parts) > 1 {
			exists, _ := strconv.Atoi(parts[1])
			resp.Success = exists == 1
		}
		
	case "ERR":
		resp.Success = false
		if len(parts) > 1 {
			resp.Error = strings.Join(parts[1:], " ")
		}
		
	default:
		// Try to parse as integer (for INCR/DECR/TTL)
		if val, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
			resp.Integer = val
			resp.TTL = val
			resp.Success = true
		} else {
			resp.Error = "unknown response type"
			resp.Success = false
		}
	}
	
	return resp, nil
}