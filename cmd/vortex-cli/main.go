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

	"github.com/vortexkv/vortexkv/internal/cluster"
	"github.com/vortexkv/vortexkv/internal/resp"
)

func main() {
	host := flag.String("h", "127.0.0.1", "Server hostname")
	port := flag.Int("p", 7379, "Server port (default: 7379)")
	authPass := flag.String("a", "", "Password for authentication")
	auto := flag.Bool("auto", false, "Execute cluster rebalance non-interactively without prompt")
	dryRun := flag.Bool("dry-run", false, "Compute and display cluster rebalance plan without executing")
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

	if *authPass != "" {
		_ = writer.WriteArrayHeader(2)
		_ = writer.WriteBulkString("AUTH")
		_ = writer.WriteBulkString(*authPass)
		if err := writer.Flush(); err == nil {
			val, err := reader.ReadValue()
			if err == nil && val.Type == resp.ErrorPrefix {
				fmt.Printf("\033[31mAuthentication failed: %s\033[0m\n", val.Str)
				os.Exit(1)
			}
		}
	}

	remainingArgs := flag.Args()

	// Special CLI Subcommand: cluster rebalance
	if len(remainingArgs) >= 2 && strings.ToLower(remainingArgs[0]) == "cluster" && strings.ToLower(remainingArgs[1]) == "rebalance" {
		handleClusterRebalance(conn, reader, writer, addr, *authPass, *auto, *dryRun)
		return
	}

	// Direct non-interactive execution: vortex-cli PING
	if len(remainingArgs) > 0 {
		start := time.Now()
		_ = writer.WriteArrayHeader(len(remainingArgs))
		for _, arg := range remainingArgs {
			_ = writer.WriteBulkString(arg)
		}
		_ = writer.Flush()
		val, err := reader.ReadValue()
		if err != nil {
			fmt.Printf("\033[31mError reading response: %v\033[0m\n", err)
			os.Exit(1)
		}
		printValue(val, 0)
		_ = start
		return
	}

	// Interactive REPL Mode
	fmt.Printf("\033[38;2;0;243;255mConnected to VortexKV at %s\033[0m (type 'quit' to exit)\n", addr)
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

func handleClusterRebalance(conn net.Conn, reader *resp.Reader, writer *resp.Writer, seedAddr, authPass string, auto, dryRun bool) {
	fmt.Printf("\033[38;2;0;243;255m=== VortexKV Distributed Cluster Rebalancer ===\033[0m\n")
	fmt.Printf("Analyzing topology from seed node %s...\n", seedAddr)

	// Send CLUSTER NODES
	_ = writer.WriteArrayHeader(2)
	_ = writer.WriteBulkString("CLUSTER")
	_ = writer.WriteBulkString("NODES")
	_ = writer.Flush()

	nodesVal, err := reader.ReadValue()
	if err != nil || nodesVal.Type == resp.ErrorPrefix {
		fmt.Printf("\033[31mFailed to query CLUSTER NODES: %v\033[0m\n", err)
		return
	}

	nodesText := string(nodesVal.Bulk)
	lines := strings.Split(nodesText, "\n")
	var masters []*cluster.MasterNodeInfo

	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}
		nodeID := parts[0]
		addrPart := parts[1]
		flags := parts[2]

		if !strings.Contains(flags, "master") {
			continue
		}

		ip := "127.0.0.1"
		port := 7379
		if atIdx := strings.Index(addrPart, "@"); atIdx != -1 {
			addrPart = addrPart[:atIdx]
		}
		if colIdx := strings.LastIndex(addrPart, ":"); colIdx != -1 {
			ip = addrPart[:colIdx]
			if p, err := strconv.Atoi(addrPart[colIdx+1:]); err == nil {
				port = p
			}
		}

		var slots []uint16
		if len(parts) >= 9 {
			slotSpec := strings.Join(parts[8:], " ")
			slots, _ = cluster.ParseSlotRanges(slotSpec)
		}

		masters = append(masters, &cluster.MasterNodeInfo{
			ID:        nodeID,
			IP:        ip,
			Port:      port,
			Slots:     slots,
			SlotCount: len(slots),
		})
	}

	if len(masters) < 2 {
		fmt.Printf("\033[33mCluster has %d master(s). Minimum 2 masters required for rebalancing.\033[0m\n", len(masters))
		return
	}

	plan, err := cluster.ComputeRebalancePlan(masters)
	if err != nil {
		fmt.Printf("\033[31mError computing rebalance plan: %v\033[0m\n", err)
		return
	}

	fmt.Printf("\nFound %d active master nodes.\n", len(masters))
	fmt.Printf("--------------------------------------------------------------------------------\n")
	fmt.Printf("%-12s %-22s %-15s %-15s %-10s\n", "NODE ID", "ADDRESS", "CURRENT SLOTS", "TARGET SLOTS", "DELTA")
	fmt.Printf("--------------------------------------------------------------------------------\n")

	for _, m := range masters {
		cur := plan.InitialCounts[m.ID]
		tgt := plan.FinalCounts[m.ID]
		delta := tgt - cur
		deltaStr := fmt.Sprintf("%+d", delta)
		if delta > 0 {
			deltaStr = "\033[32m" + deltaStr + "\033[0m"
		} else if delta < 0 {
			deltaStr = "\033[31m" + deltaStr + "\033[0m"
		} else {
			deltaStr = "\033[90m0\033[0m"
		}
		addrStr := fmt.Sprintf("%s:%d", m.IP, m.Port)
		fmt.Printf("%-12s %-22s %-15d %-15d %-10s\n", m.ID[:8], addrStr, cur, tgt, deltaStr)
	}
	fmt.Printf("--------------------------------------------------------------------------------\n")
	fmt.Printf("Total slots to migrate: \033[33m%d slots\033[0m\n\n", plan.TotalSlotsToMove)

	if plan.TotalSlotsToMove == 0 {
		fmt.Printf("\033[32mCluster is already perfectly balanced! No action needed.\033[0m\n")
		return
	}

	if dryRun {
		fmt.Printf("\033[36m[DRY RUN] Plan calculated successfully. No changes applied.\033[0m\n")
		return
	}

	if !auto {
		fmt.Print("Proceed with slot rebalancing? [y/N]: ")
		var resp string
		_, _ = fmt.Scanln(&resp)
		if strings.ToLower(resp) != "y" && strings.ToLower(resp) != "yes" {
			fmt.Println("Aborted by user.")
			return
		}
	}

	fmt.Printf("Executing slot migration across nodes...\n")
	start := time.Now()
	err = cluster.ExecuteRebalancePlan(seedAddr, authPass, plan, func(done, total int, move cluster.SlotMove) {
		pct := float64(done) / float64(total) * 100.0
		fmt.Printf("\rMigrating slot %d [%s -> %s] (%d/%d - %.1f%%)...",
			move.Slot, move.FromNodeID[:8], move.ToNodeID[:8], done, total, pct)
	})

	if err != nil {
		fmt.Printf("\n\033[31mRebalance failed: %v\033[0m\n", err)
		return
	}

	fmt.Printf("\n\033[32m✓ Cluster rebalance successfully completed in %v! All %d slots redistributed.\033[0m\n",
		time.Since(start).Round(time.Millisecond), plan.TotalSlotsToMove)
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
