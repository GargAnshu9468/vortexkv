package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vortexkv/vortexkv/internal/resp"
)

func main() {
	host := flag.String("h", "127.0.0.1", "Server hostname")
	port := flag.Int("p", 7379, "Server port (default: 7379)")
	authPass := flag.String("a", "", "Password for authentication")
	flag.Parse()

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("\033[31mCould not connect to VortexKV at %s: %v\033[0m\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	fmt.Printf("\033[38;2;0;243;255mConnected to VortexKV at %s\033[0m (type 'quit' to exit)\n", addr)

	if *authPass != "" {
		_ = writer.WriteArrayHeader(2)
		_ = writer.WriteBulkString("AUTH")
		_ = writer.WriteBulkString(*authPass)
		if err := writer.Flush(); err == nil {
			val, err := reader.ReadValue()
			if err == nil {
				if val.Type == resp.ErrorPrefix {
					fmt.Printf("\033[31mAuthentication failed: %s\033[0m\n", val.Str)
				} else {
					fmt.Printf("\033[32mAuthenticated successfully.\033[0m\n")
				}
			}
		}
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("\033[38;2;138;43;226mvortex\033[0m:\033[38;2;57;255;20m%d\033[0m> ", *port)
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.ToLower(line) == "quit" || strings.ToLower(line) == "exit" {
			break
		}

		args := parseInput(line)
		if len(args) == 0 {
			continue
		}

		start := time.Now()
		// Write command array
		_ = writer.WriteArrayHeader(len(args))
		for _, arg := range args {
			_ = writer.WriteBulkString(arg)
		}
		if err := writer.Flush(); err != nil {
			fmt.Printf("\033[31mError writing command: %v\033[0m\n", err)
			break
		}

		val, err := reader.ReadValue()
		duration := time.Since(start)
		if err != nil {
			fmt.Printf("\033[31mError reading response: %v\033[0m\n", err)
			break
		}

		printValue(val, 0)
		fmt.Printf("\033[90m(%.2fms / %dµs)\033[0m\n", float64(duration.Microseconds())/1000.0, duration.Microseconds())
	}
}

func printValue(v resp.Value, indent int) {
	pad := strings.Repeat("  ", indent)
	switch v.Type {
	case resp.SimpleStringPrefix:
		fmt.Printf("%s\033[32m%s\033[0m\n", pad, v.Str)
	case resp.ErrorPrefix:
		fmt.Printf("%s\033[31m(error) %s\033[0m\n", pad, v.Str)
	case resp.IntegerPrefix:
		fmt.Printf("%s\033[36m(integer) %d\033[0m\n", pad, v.Num)
	case resp.BulkStringPrefix:
		if v.Null {
			fmt.Printf("%s\033[90m(nil)\033[0m\n", pad)
		} else {
			fmt.Printf("%s\033[33m\"%s\"\033[0m\n", pad, string(v.Bulk))
		}
	case resp.ArrayPrefix:
		if v.Null {
			fmt.Printf("%s\033[90m(nil)\033[0m\n", pad)
		} else if len(v.Array) == 0 {
			fmt.Printf("%s\033[90m(empty array)\033[0m\n", pad)
		} else {
			for i, item := range v.Array {
				fmt.Printf("%s%d) ", pad, i+1)
				printValue(item, indent+1)
			}
		}
	}
}

func parseInput(cmd string) []string {
	var args []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		if inQuotes {
			if c == quoteChar {
				inQuotes = false
			} else if c == '\\' && i+1 < len(cmd) {
				i++
				current.WriteByte(cmd[i])
			} else {
				current.WriteByte(c)
			}
		} else {
			if c == '"' || c == '\'' {
				inQuotes = true
				quoteChar = c
			} else if c == ' ' || c == '\t' {
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
			} else {
				current.WriteByte(c)
			}
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args
}
