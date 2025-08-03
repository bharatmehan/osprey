package server

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/alignecoderepos/osprey/internal/protocol"
	"github.com/alignecoderepos/osprey/internal/storage"
)

// handlePing handles the PING command
func (s *Server) handlePing(w io.Writer) {
	protocol.WritePong(w)
}

// handleGet handles the GET command
func (s *Server) handleGet(cmd *protocol.Command, w io.Writer) {
	if len(cmd.Args) != 1 {
		protocol.WriteError(w, "BADREQ", "GET requires 1 argument")
		return
	}

	key := cmd.Args[0]
	entry, err := s.store.Get(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			protocol.WriteNotFound(w)
		} else {
			protocol.WriteError(w, "INTERNAL", err.Error())
		}
		return
	}

	protocol.WriteValue(w, len(entry.Value), entry.Version, entry.ExpiryMs, entry.Value)
}

// handleSet handles the SET command
func (s *Server) handleSet(cmd *protocol.Command, w io.Writer) {
	if len(cmd.Args) < 2 {
		protocol.WriteError(w, "BADREQ", "SET requires at least 2 arguments")
		return
	}

	key := cmd.Args[0]

	// Parse options
	opts := storage.SetOptions{}
	i := 2 // Start after key and length

	for i < len(cmd.Args) {
		arg := strings.ToUpper(cmd.Args[i])
		switch arg {
		case "EX":
			if i+1 >= len(cmd.Args) {
				protocol.WriteError(w, "BADREQ", "EX requires value")
				return
			}
			ttl, err := strconv.ParseInt(cmd.Args[i+1], 10, 64)
			if err != nil {
				protocol.WriteError(w, "BADREQ", "invalid TTL")
				return
			}
			opts.ExpiryMs = ttl
			i += 2

		case "PXAT":
			if i+1 >= len(cmd.Args) {
				protocol.WriteError(w, "BADREQ", "PXAT requires value")
				return
			}
			absMs, err := strconv.ParseInt(cmd.Args[i+1], 10, 64)
			if err != nil {
				protocol.WriteError(w, "BADREQ", "invalid absolute expiry")
				return
			}
			opts.AbsoluteExpiryMs = absMs
			i += 2

		case "NX":
			opts.NX = true
			i++

		case "XX":
			opts.XX = true
			i++

		case "VER":
			if i+1 >= len(cmd.Args) {
				protocol.WriteError(w, "BADREQ", "VER requires value")
				return
			}
			ver, err := strconv.ParseUint(cmd.Args[i+1], 10, 64)
			if err != nil {
				protocol.WriteError(w, "BADREQ", "invalid version")
				return
			}
			opts.CheckVersion = true
			opts.Version = ver
			i += 2

		default:
			protocol.WriteError(w, "BADREQ", fmt.Sprintf("unknown option: %s", arg))
			return
		}
	}

	// Check for conflicting options
	if opts.ExpiryMs > 0 && opts.AbsoluteExpiryMs > 0 {
		protocol.WriteError(w, "BADREQ", "EX and PXAT are mutually exclusive")
		return
	}

	// Set the value
	version, err := s.store.Set(key, cmd.Payload, opts)
	if err != nil {
		switch err {
		case storage.ErrKeyExists:
			protocol.WriteError(w, "EXISTS", "key already exists")
		case storage.ErrKeyNotFound:
			protocol.WriteError(w, "NEXISTS", "key does not exist")
		case storage.ErrVersionMismatch:
			protocol.WriteError(w, "VER", "version mismatch")
		case storage.ErrKeyTooLarge:
			protocol.WriteError(w, "TOOLARGE", "key too large")
		case storage.ErrValueTooLarge:
			protocol.WriteError(w, "TOOLARGE", "value too large")
		default:
			protocol.WriteError(w, "INTERNAL", err.Error())
		}
		return
	}

	protocol.WriteOKWithVersion(w, version)
}

// handleDel handles the DEL command
func (s *Server) handleDel(cmd *protocol.Command, w io.Writer) {
	if len(cmd.Args) != 1 {
		protocol.WriteError(w, "BADREQ", "DEL requires 1 argument")
		return
	}

	key := cmd.Args[0]
	deleted := s.store.Delete(key)
	protocol.WriteDeleted(w, deleted)
}

// handleExists handles the EXISTS command
func (s *Server) handleExists(cmd *protocol.Command, w io.Writer) {
	if len(cmd.Args) != 1 {
		protocol.WriteError(w, "BADREQ", "EXISTS requires 1 argument")
		return
	}

	key := cmd.Args[0]
	exists := s.store.Exists(key)
	protocol.WriteExists(w, exists)
}

// handleExpire handles the EXPIRE command
func (s *Server) handleExpire(cmd *protocol.Command, w io.Writer) {
	if len(cmd.Args) != 2 {
		protocol.WriteError(w, "BADREQ", "EXPIRE requires 2 arguments")
		return
	}

	key := cmd.Args[0]
	ttlMs, err := strconv.ParseInt(cmd.Args[1], 10, 64)
	if err != nil {
		protocol.WriteError(w, "BADREQ", "invalid TTL")
		return
	}

	err = s.store.Expire(key, ttlMs)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			protocol.WriteNotFound(w)
		} else {
			protocol.WriteError(w, "INTERNAL", err.Error())
		}
		return
	}

	protocol.WriteOK(w)
}

// handleTTL handles the TTL command
func (s *Server) handleTTL(cmd *protocol.Command, w io.Writer) {
	if len(cmd.Args) != 1 {
		protocol.WriteError(w, "BADREQ", "TTL requires 1 argument")
		return
	}

	key := cmd.Args[0]
	ttl := s.store.TTL(key)
	protocol.WriteTTL(w, ttl)
}

// handleIncr handles INCR/DECR commands
func (s *Server) handleIncr(cmd *protocol.Command, w io.Writer, sign int64) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		protocol.WriteError(w, "BADREQ", fmt.Sprintf("%s requires 1 or 2 arguments", cmd.Name))
		return
	}

	key := cmd.Args[0]
	delta := int64(1)

	if len(cmd.Args) == 2 {
		d, err := strconv.ParseInt(cmd.Args[1], 10, 64)
		if err != nil {
			protocol.WriteError(w, "BADREQ", "invalid delta")
			return
		}
		delta = d
	}

	newVal, err := s.store.Incr(key, delta*sign)
	if err != nil {
		if err == storage.ErrNotInteger {
			protocol.WriteError(w, "TYPE", "value is not an integer")
		} else {
			protocol.WriteError(w, "INTERNAL", err.Error())
		}
		return
	}

	protocol.WriteInteger(w, newVal)
}

// handleStats handles the STATS command
func (s *Server) handleStats(cmd *protocol.Command, w io.Writer) {
	stats := s.store.GetStats()

	// Add server-level stats
	stats["clients"] = strconv.Itoa(int(s.clientCount))

	// Add WAL stats
	walStats := s.store.GetWALStats()
	for k, v := range walStats {
		stats[k] = v
	}

	// Write stats
	for k, v := range stats {
		fmt.Fprintf(w, "%s=%s\r\n", k, v)
	}
	fmt.Fprintf(w, "END\r\n")
}

// handleMGet handles the MGET command
func (s *Server) handleMGet(cmd *protocol.Command, w io.Writer) {
	if len(cmd.Args) == 0 {
		protocol.WriteError(w, "BADREQ", "MGET requires at least 1 argument")
		return
	}

	for _, key := range cmd.Args {
		entry, err := s.store.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				fmt.Fprintf(w, "NOT_FOUND %s\r\n", key)
			} else {
				protocol.WriteError(w, "INTERNAL", err.Error())
				return
			}
			continue
		}

		fmt.Fprintf(w, "VALUE %s %d %d %d\r\n", key, len(entry.Value), entry.Version, entry.ExpiryMs)
		w.Write(entry.Value)
		w.Write([]byte("\r\n"))
	}
}

// handleMSet handles the MSET command
func (s *Server) handleMSet(cmd *protocol.Command, w io.Writer) {
	// MSET k1 len1 k2 len2 ...
	if len(cmd.Args) == 0 || len(cmd.Args)%2 != 0 {
		protocol.WriteError(w, "BADREQ", "MSET requires even number of arguments")
		return
	}

	// Parse keys and lengths
	var keys []string
	var lengths []int
	offset := 0

	for i := 0; i < len(cmd.Args); i += 2 {
		key := cmd.Args[i]
		length, err := strconv.Atoi(cmd.Args[i+1])
		if err != nil || length < 0 {
			protocol.WriteError(w, "BADREQ", "invalid length")
			return
		}

		keys = append(keys, key)
		lengths = append(lengths, length)
	}

	// Set each key-value pair
	count := 0
	for i, key := range keys {
		length := lengths[i]
		value := cmd.Payload[offset : offset+length]
		offset += length

		_, err := s.store.Set(key, value, storage.SetOptions{})
		if err != nil {
			if err == storage.ErrKeyTooLarge || err == storage.ErrValueTooLarge {
				protocol.WriteError(w, "TOOLARGE", err.Error())
			} else {
				protocol.WriteError(w, "INTERNAL", err.Error())
			}
			return
		}
		count++
	}

	fmt.Fprintf(w, "OK %d\r\n", count)
}
