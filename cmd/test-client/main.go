package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:7070")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Test PING
	fmt.Println("Testing PING...")
	writer.WriteString("PING\r\n")
	writer.Flush()
	resp, _ := reader.ReadString('\n')
	fmt.Printf("Response: %s", resp)

	// Test SET
	fmt.Println("\nTesting SET...")
	writer.WriteString("SET user:1 5\r\nalice\r\n")
	writer.Flush()
	resp, _ = reader.ReadString('\n')
	fmt.Printf("Response: %s", resp)

	// Test GET
	fmt.Println("\nTesting GET...")
	writer.WriteString("GET user:1\r\n")
	writer.Flush()
	resp, _ = reader.ReadString('\n')
	fmt.Printf("Response: %s", resp)
	if strings.HasPrefix(resp, "VALUE") {
		// Read the value
		value := make([]byte, 5)
		reader.Read(value)
		fmt.Printf("Value: %s\n", value)
		reader.ReadString('\n') // Read trailing \r\n
	}

	// Test INCR
	fmt.Println("\nTesting INCR...")
	writer.WriteString("INCR counter 10\r\n")
	writer.Flush()
	resp, _ = reader.ReadString('\n')
	fmt.Printf("Response: %s", resp)

	// Test STATS
	fmt.Println("\nTesting STATS...")
	writer.WriteString("STATS\r\n")
	writer.Flush()
	for {
		resp, _ = reader.ReadString('\n')
		fmt.Printf("%s", resp)
		if strings.TrimSpace(resp) == "END" {
			break
		}
	}
}