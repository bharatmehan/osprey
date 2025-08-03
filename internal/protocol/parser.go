package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	ErrInvalidCommand = errors.New("invalid command")
	ErrInvalidArgs    = errors.New("invalid arguments")
	ErrInvalidPayload = errors.New("invalid payload")
)

// Command represents a parsed command
type Command struct {
	Name    string
	Args    []string
	Payload []byte
}

// Parser handles protocol parsing
type Parser struct {
	reader *bufio.Reader
}

// NewParser creates a new protocol parser
func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(r),
	}
}

// ParseCommand parses a single command from the input
func (p *Parser) ParseCommand() (*Command, error) {
	// Read command line
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	// Trim \r\n
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")

	if line == "" {
		return nil, ErrInvalidCommand
	}

	// Split into parts
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, ErrInvalidCommand
	}

	cmd := &Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}

	// Check if command requires payload
	if cmd.requiresPayload() {
		payload, err := p.readPayload(cmd)
		if err != nil {
			return nil, err
		}
		cmd.Payload = payload
	}

	return cmd, nil
}

// requiresPayload checks if the command requires a payload
func (cmd *Command) requiresPayload() bool {
	switch cmd.Name {
	case "SET":
		return true
	case "MSET":
		return true
	default:
		return false
	}
}

// readPayload reads the payload for commands that require it
func (p *Parser) readPayload(cmd *Command) ([]byte, error) {
	switch cmd.Name {
	case "SET":
		return p.readSinglePayload(cmd)
	case "MSET":
		return p.readMultiPayload(cmd)
	default:
		return nil, nil
	}
}

// readSinglePayload reads a single payload for SET command
func (p *Parser) readSinglePayload(cmd *Command) ([]byte, error) {
	if len(cmd.Args) < 2 {
		return nil, ErrInvalidArgs
	}

	// Parse length from args[1]
	length, err := strconv.Atoi(cmd.Args[1])
	if err != nil || length < 0 {
		return nil, ErrInvalidArgs
	}

	// Read the payload
	payload := make([]byte, length)
	_, err = io.ReadFull(p.reader, payload)
	if err != nil {
		return nil, err
	}

	// Read the trailing \r\n
	crlf := make([]byte, 2)
	_, err = io.ReadFull(p.reader, crlf)
	if err != nil {
		return nil, err
	}

	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, ErrInvalidPayload
	}

	return payload, nil
}

// readMultiPayload reads multiple payloads for MSET command
func (p *Parser) readMultiPayload(cmd *Command) ([]byte, error) {
	// MSET format: MSET k1 len1 k2 len2 ...
	// Followed by concatenated payloads

	if len(cmd.Args)%2 != 0 {
		return nil, ErrInvalidArgs
	}

	totalLength := 0
	lengths := []int{}

	// Parse all lengths
	for i := 1; i < len(cmd.Args); i += 2 {
		length, err := strconv.Atoi(cmd.Args[i])
		if err != nil || length < 0 {
			return nil, ErrInvalidArgs
		}
		lengths = append(lengths, length)
		totalLength += length
	}

	// Read all payloads at once
	payload := make([]byte, totalLength)
	_, err := io.ReadFull(p.reader, payload)
	if err != nil {
		return nil, err
	}

	// Read the trailing \r\n
	crlf := make([]byte, 2)
	_, err = io.ReadFull(p.reader, crlf)
	if err != nil {
		return nil, err
	}

	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, ErrInvalidPayload
	}

	return payload, nil
}

// Response helpers

// WriteError writes an error response
func WriteError(w io.Writer, code, message string) error {
	_, err := fmt.Fprintf(w, "ERR %s %s\r\n", code, message)
	return err
}

// WriteOK writes an OK response
func WriteOK(w io.Writer) error {
	_, err := w.Write([]byte("OK\r\n"))
	return err
}

// WriteOKWithVersion writes an OK response with version
func WriteOKWithVersion(w io.Writer, version uint64) error {
	_, err := fmt.Fprintf(w, "OK %d\r\n", version)
	return err
}

// WritePong writes a PONG response
func WritePong(w io.Writer) error {
	_, err := w.Write([]byte("PONG\r\n"))
	return err
}

// WriteNotFound writes a NOT_FOUND response
func WriteNotFound(w io.Writer) error {
	_, err := w.Write([]byte("NOT_FOUND\r\n"))
	return err
}

// WriteValue writes a VALUE response with payload
func WriteValue(w io.Writer, length int, version uint64, expiryMs int64, value []byte) error {
	_, err := fmt.Fprintf(w, "VALUE %d %d %d\r\n", length, version, expiryMs)
	if err != nil {
		return err
	}

	_, err = w.Write(value)
	if err != nil {
		return err
	}

	_, err = w.Write([]byte("\r\n"))
	return err
}

// WriteDeleted writes a DELETED response
func WriteDeleted(w io.Writer, deleted bool) error {
	val := 0
	if deleted {
		val = 1
	}
	_, err := fmt.Fprintf(w, "DELETED %d\r\n", val)
	return err
}

// WriteExists writes an EXISTS response
func WriteExists(w io.Writer, exists bool) error {
	val := 0
	if exists {
		val = 1
	}
	_, err := fmt.Fprintf(w, "EXISTS %d\r\n", val)
	return err
}

// WriteTTL writes a TTL response
func WriteTTL(w io.Writer, ttl int64) error {
	_, err := fmt.Fprintf(w, "%d\r\n", ttl)
	return err
}

// WriteInteger writes an integer response (for INCR/DECR)
func WriteInteger(w io.Writer, value int64) error {
	_, err := fmt.Fprintf(w, "%d\r\n", value)
	return err
}
