package protocol

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_ParseCommand_Simple(t *testing.T) {
	tests := []struct {
		input    string
		expected *Command
	}{
		{
			input: "PING\r\n",
			expected: &Command{
				Name: "PING",
				Args: []string{},
			},
		},
		{
			input: "GET key1\r\n",
			expected: &Command{
				Name: "GET",
				Args: []string{"key1"},
			},
		},
		{
			input: "DEL key1\r\n",
			expected: &Command{
				Name: "DEL",
				Args: []string{"key1"},
			},
		},
		{
			input: "EXISTS key1\r\n",
			expected: &Command{
				Name: "EXISTS",
				Args: []string{"key1"},
			},
		},
		{
			input: "EXPIRE key1 1000\r\n",
			expected: &Command{
				Name: "EXPIRE",
				Args: []string{"key1", "1000"},
			},
		},
		{
			input: "TTL key1\r\n",
			expected: &Command{
				Name: "TTL",
				Args: []string{"key1"},
			},
		},
		{
			input: "INCR counter 5\r\n",
			expected: &Command{
				Name: "INCR",
				Args: []string{"counter", "5"},
			},
		},
		{
			input: "MGET key1 key2 key3\r\n",
			expected: &Command{
				Name: "MGET",
				Args: []string{"key1", "key2", "key3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			parser := NewParser(strings.NewReader(tt.input))
			cmd, err := parser.ParseCommand()
			require.NoError(t, err)
			assert.Equal(t, tt.expected.Name, cmd.Name)
			assert.Equal(t, tt.expected.Args, cmd.Args)
			assert.Nil(t, cmd.Payload)
		})
	}
}

func TestParser_ParseCommand_SET(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *Command
	}{
		{
			name:  "Basic SET",
			input: "SET key1 5\r\nhello\r\n",
			expected: &Command{
				Name:    "SET",
				Args:    []string{"key1", "5"},
				Payload: []byte("hello"),
			},
		},
		{
			name:  "SET with options",
			input: "SET key1 5 EX 1000 NX\r\nhello\r\n",
			expected: &Command{
				Name:    "SET",
				Args:    []string{"key1", "5", "EX", "1000", "NX"},
				Payload: []byte("hello"),
			},
		},
		{
			name:  "SET with empty value",
			input: "SET key1 0\r\n\r\n",
			expected: &Command{
				Name:    "SET",
				Args:    []string{"key1", "0"},
				Payload: []byte(""),
			},
		},
		{
			name:  "SET with binary data",
			input: "SET key1 4\r\n\x00\x01\x02\x03\r\n",
			expected: &Command{
				Name:    "SET",
				Args:    []string{"key1", "4"},
				Payload: []byte{0, 1, 2, 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(strings.NewReader(tt.input))
			cmd, err := parser.ParseCommand()
			require.NoError(t, err)
			assert.Equal(t, tt.expected.Name, cmd.Name)
			assert.Equal(t, tt.expected.Args, cmd.Args)
			assert.Equal(t, tt.expected.Payload, cmd.Payload)
		})
	}
}

func TestParser_ParseCommand_MSET(t *testing.T) {
	input := "MSET key1 5 key2 3\r\nhellobar\r\n"
	expected := &Command{
		Name:    "MSET",
		Args:    []string{"key1", "5", "key2", "3"},
		Payload: []byte("hellobar"),
	}

	parser := NewParser(strings.NewReader(input))
	cmd, err := parser.ParseCommand()
	require.NoError(t, err)
	assert.Equal(t, expected.Name, cmd.Name)
	assert.Equal(t, expected.Args, cmd.Args)
	assert.Equal(t, expected.Payload, cmd.Payload)
}

func TestParser_ParseCommand_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Empty line",
			input: "\r\n",
		},
		{
			name:  "SET with invalid length",
			input: "SET key1 abc\r\nhello\r\n",
		},
		{
			name:  "SET with negative length",
			input: "SET key1 -1\r\nhello\r\n",
		},
		{
			name:  "SET missing payload",
			input: "SET key1 5\r\n",
		},
		{
			name:  "MSET with odd number of args",
			input: "MSET key1 5 key2\r\nhello\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(strings.NewReader(tt.input))
			_, err := parser.ParseCommand()
			assert.Error(t, err)
		})
	}
}

func TestParser_CaseInsensitive(t *testing.T) {
	tests := []string{
		"ping\r\n",
		"PING\r\n",
		"Ping\r\n",
		"pInG\r\n",
	}

	for _, input := range tests {
		parser := NewParser(strings.NewReader(input))
		cmd, err := parser.ParseCommand()
		require.NoError(t, err)
		assert.Equal(t, "PING", cmd.Name)
	}
}

func TestResponseWriters(t *testing.T) {
	tests := []struct {
		name     string
		writer   func() ([]byte, error)
		expected string
	}{
		{
			name: "WriteOK",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteOK(&buf)
				return buf.Bytes(), err
			},
			expected: "OK\r\n",
		},
		{
			name: "WriteOKWithVersion",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteOKWithVersion(&buf, 42)
				return buf.Bytes(), err
			},
			expected: "OK 42\r\n",
		},
		{
			name: "WritePong",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WritePong(&buf)
				return buf.Bytes(), err
			},
			expected: "PONG\r\n",
		},
		{
			name: "WriteNotFound",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteNotFound(&buf)
				return buf.Bytes(), err
			},
			expected: "NOT_FOUND\r\n",
		},
		{
			name: "WriteError",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteError(&buf, "BADREQ", "invalid command")
				return buf.Bytes(), err
			},
			expected: "ERR BADREQ invalid command\r\n",
		},
		{
			name: "WriteDeleted true",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteDeleted(&buf, true)
				return buf.Bytes(), err
			},
			expected: "DELETED 1\r\n",
		},
		{
			name: "WriteDeleted false",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteDeleted(&buf, false)
				return buf.Bytes(), err
			},
			expected: "DELETED 0\r\n",
		},
		{
			name: "WriteExists true",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteExists(&buf, true)
				return buf.Bytes(), err
			},
			expected: "EXISTS 1\r\n",
		},
		{
			name: "WriteTTL",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteTTL(&buf, 1234)
				return buf.Bytes(), err
			},
			expected: "1234\r\n",
		},
		{
			name: "WriteInteger",
			writer: func() ([]byte, error) {
				var buf bytes.Buffer
				err := WriteInteger(&buf, -42)
				return buf.Bytes(), err
			},
			expected: "-42\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.writer()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestWriteValue(t *testing.T) {
	var buf bytes.Buffer
	value := []byte("hello world")
	err := WriteValue(&buf, len(value), 42, 1234567890, value)
	require.NoError(t, err)

	expected := "VALUE 11 42 1234567890\r\nhello world\r\n"
	assert.Equal(t, expected, buf.String())
}
