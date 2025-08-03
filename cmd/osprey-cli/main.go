package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/alignecoderepos/osprey/pkg/client"
)

func main() {
	var (
		address = flag.String("addr", "localhost:7070", "Server address")
		output  = flag.String("out", "", "Output file for binary values")
		input   = flag.String("in", "", "Input file for binary values (use '-' for stdin)")
	)
	flag.Parse()

	if len(flag.Args()) == 0 {
		fmt.Println("Usage: osprey-cli [options] <command> [args...]")
		fmt.Println("\nCommands:")
		fmt.Println("  ping")
		fmt.Println("  get <key>")
		fmt.Println("  set <key> <value> [EX <ms>] [PXAT <ms>] [NX|XX] [VER <n>]")
		fmt.Println("  del <key>")
		fmt.Println("  exists <key>")
		fmt.Println("  expire <key> <ttl_ms>")
		fmt.Println("  ttl <key>")
		fmt.Println("  incr <key> [delta]")
		fmt.Println("  decr <key> [delta]")
		fmt.Println("  mget <key1> <key2> ...")
		fmt.Println("  stats")
		fmt.Println("\nOptions:")
		fmt.Println("  -addr string    Server address (default \"localhost:7070\")")
		fmt.Println("  -in string      Input file for binary values (use '-' for stdin)")
		fmt.Println("  -out string     Output file for binary values")
		os.Exit(1)
	}

	c, err := client.New(*address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	cmd := strings.ToLower(flag.Args()[0])
	args := flag.Args()[1:]

	switch cmd {
	case "ping":
		handlePing(c)
	case "get":
		handleGet(c, args, *output)
	case "set":
		handleSet(c, args, *input)
	case "del":
		handleDel(c, args)
	case "exists":
		handleExists(c, args)
	case "expire":
		handleExpire(c, args)
	case "ttl":
		handleTTL(c, args)
	case "incr":
		handleIncr(c, args)
	case "decr":
		handleDecr(c, args)
	case "mget":
		handleMGet(c, args, *output)
	case "stats":
		handleStats(c)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		os.Exit(1)
	}
}

func handlePing(c *client.Client) {
	if err := c.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PONG")
}

func handleGet(c *client.Client, args []string, outputFile string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: get <key>\n")
		os.Exit(1)
	}

	resp, err := c.Get(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if !resp.Success {
		fmt.Println("NOT_FOUND")
		return
	}

	fmt.Printf("VALUE %d %d %d\n", len(resp.Value), resp.Version, resp.ExpiryMs)

	if outputFile != "" {
		err := os.WriteFile(outputFile, resp.Value, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write output file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Value written to %s\n", outputFile)
	} else {
		os.Stdout.Write(resp.Value)
		fmt.Println()
	}
}

func handleSet(c *client.Client, args []string, inputFile string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: set <key> <value> [options...]\n")
		os.Exit(1)
	}

	key := args[0]
	var value []byte
	var options []string

	if inputFile != "" {
		var err error
		if inputFile == "-" {
			value, err = io.ReadAll(os.Stdin)
		} else {
			value, err = os.ReadFile(inputFile)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read input: %v\n", err)
			os.Exit(1)
		}
		options = args[1:]
	} else {
		value = []byte(args[1])
		options = args[2:]
	}

	resp, err := c.Set(key, value, options...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Printf("OK %d\n", resp.Version)
	} else {
		fmt.Printf("ERR %s\n", resp.Error)
		os.Exit(1)
	}
}

func handleDel(c *client.Client, args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: del <key>\n")
		os.Exit(1)
	}

	resp, err := c.Del(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Println("DELETED 1")
	} else {
		fmt.Println("DELETED 0")
	}
}

func handleExists(c *client.Client, args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: exists <key>\n")
		os.Exit(1)
	}

	resp, err := c.Exists(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Println("EXISTS 1")
	} else {
		fmt.Println("EXISTS 0")
	}
}

func handleExpire(c *client.Client, args []string) {
	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: expire <key> <ttl_ms>\n")
		os.Exit(1)
	}

	ttl, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid TTL: %v\n", err)
		os.Exit(1)
	}

	resp, err := c.Expire(args[0], ttl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Println("OK")
	} else {
		fmt.Println("NOT_FOUND")
	}
}

func handleTTL(c *client.Client, args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: ttl <key>\n")
		os.Exit(1)
	}

	resp, err := c.TTL(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(resp.TTL)
}

func handleIncr(c *client.Client, args []string) {
	if len(args) < 1 || len(args) > 2 {
		fmt.Fprintf(os.Stderr, "Usage: incr <key> [delta]\n")
		os.Exit(1)
	}

	var delta []int64
	if len(args) == 2 {
		d, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid delta: %v\n", err)
			os.Exit(1)
		}
		delta = []int64{d}
	}

	resp, err := c.Incr(args[0], delta...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Println(resp.Integer)
	} else {
		fmt.Printf("ERR %s\n", resp.Error)
		os.Exit(1)
	}
}

func handleDecr(c *client.Client, args []string) {
	if len(args) < 1 || len(args) > 2 {
		fmt.Fprintf(os.Stderr, "Usage: decr <key> [delta]\n")
		os.Exit(1)
	}

	var delta []int64
	if len(args) == 2 {
		d, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid delta: %v\n", err)
			os.Exit(1)
		}
		delta = []int64{d}
	}

	resp, err := c.Decr(args[0], delta...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Success {
		fmt.Println(resp.Integer)
	} else {
		fmt.Printf("ERR %s\n", resp.Error)
		os.Exit(1)
	}
}

func handleMGet(c *client.Client, args []string, outputFile string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: mget <key1> <key2> ...\n")
		os.Exit(1)
	}

	responses, err := c.MGet(args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	for i, resp := range responses {
		if resp.Success {
			fmt.Printf("VALUE %s %d %d %d\n", args[i], len(resp.Value), resp.Version, resp.ExpiryMs)
			os.Stdout.Write(resp.Value)
			fmt.Println()
		} else {
			fmt.Printf("NOT_FOUND %s\n", args[i])
		}
	}
}

func handleStats(c *client.Client) {
	stats, err := c.Stats()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	for key, value := range stats {
		fmt.Printf("%s=%s\n", key, value)
	}
	fmt.Println("END")
}
