package cluster

import (
	"bufio"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MasterNodeInfo holds slot statistics for a cluster master node.
type MasterNodeInfo struct {
	ID        string
	IP        string
	Port      int
	BusPort   int
	Slots     []uint16
	SlotCount int
}

// SlotMove defines a single slot migration from donor to receiver.
type SlotMove struct {
	Slot       uint16
	FromNodeID string
	FromAddr   string
	ToNodeID   string
	ToAddr     string
}

// RebalancePlan details all slot migrations required to balance a cluster.
type RebalancePlan struct {
	TotalSlotsToMove int
	Moves            []SlotMove
	InitialCounts    map[string]int
	FinalCounts      map[string]int
}

// ComputeRebalancePlan calculates the optimal slot migration plan to equalize slot distribution.
func ComputeRebalancePlan(masters []*MasterNodeInfo) (*RebalancePlan, error) {
	if len(masters) < 2 {
		return nil, fmt.Errorf("rebalance requires at least 2 master nodes")
	}

	totalMasters := len(masters)
	targetBase := 16384 / totalMasters
	remainder := 16384 % totalMasters

	// Deterministically sort masters by ID for reproducible plans
	sortedMasters := make([]*MasterNodeInfo, len(masters))
	copy(sortedMasters, masters)
	sort.Slice(sortedMasters, func(i, j int) bool {
		return sortedMasters[i].ID < sortedMasters[j].ID
	})

	initialCounts := make(map[string]int)
	targetCounts := make(map[string]int)
	finalCounts := make(map[string]int)

	for i, m := range sortedMasters {
		initialCounts[m.ID] = len(m.Slots)
		finalCounts[m.ID] = len(m.Slots)
		target := targetBase
		if i < remainder {
			target++
		}
		targetCounts[m.ID] = target
	}

	type donor struct {
		m     *MasterNodeInfo
		avail []uint16
	}
	type receiver struct {
		m      *MasterNodeInfo
		needed int
	}

	var donors []*donor
	var receivers []*receiver

	for _, m := range sortedMasters {
		cur := initialCounts[m.ID]
		tgt := targetCounts[m.ID]
		if cur > tgt {
			excess := cur - tgt
			avail := make([]uint16, excess)
			// Take excess slots from the end of the node's slot list
			copy(avail, m.Slots[len(m.Slots)-excess:])
			donors = append(donors, &donor{m: m, avail: avail})
		} else if cur < tgt {
			receivers = append(receivers, &receiver{m: m, needed: tgt - cur})
		}
	}

	var moves []SlotMove
	dIdx := 0

	for _, rec := range receivers {
		for rec.needed > 0 && dIdx < len(donors) {
			d := donors[dIdx]
			if len(d.avail) == 0 {
				dIdx++
				continue
			}

			take := rec.needed
			if len(d.avail) < take {
				take = len(d.avail)
			}

			for i := 0; i < take; i++ {
				slot := d.avail[i]
				moves = append(moves, SlotMove{
					Slot:       slot,
					FromNodeID: d.m.ID,
					FromAddr:   fmt.Sprintf("%s:%d", d.m.IP, d.m.Port),
					ToNodeID:   rec.m.ID,
					ToAddr:     fmt.Sprintf("%s:%d", rec.m.IP, rec.m.Port),
				})
				finalCounts[d.m.ID]--
				finalCounts[rec.m.ID]++
			}

			d.avail = d.avail[take:]
			rec.needed -= take

			if len(d.avail) == 0 {
				dIdx++
			}
		}
	}

	return &RebalancePlan{
		TotalSlotsToMove: len(moves),
		Moves:            moves,
		InitialCounts:    initialCounts,
		FinalCounts:      finalCounts,
	}, nil
}

// ExecuteRebalancePlan applies the computed migration plan to live cluster instances.
func ExecuteRebalancePlan(seedAddr string, authPass string, plan *RebalancePlan, progressCb func(done, total int, move SlotMove)) error {
	if len(plan.Moves) == 0 {
		return nil
	}

	// Connect to seed to discover all cluster master endpoints
	conn, err := net.DialTimeout("tcp", seedAddr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to cluster seed %s: %w", seedAddr, err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	if authPass != "" {
		_, _ = fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(authPass), authPass)
		_, _ = reader.ReadString('\n')
	}

	// Cache node connections
	nodeConns := make(map[string]net.Conn)
	defer func() {
		for _, c := range nodeConns {
			_ = c.Close()
		}
	}()

	getConn := func(addr string) (net.Conn, *bufio.Reader, error) {
		if c, ok := nodeConns[addr]; ok {
			return c, bufio.NewReader(c), nil
		}
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			return nil, nil, err
		}
		r := bufio.NewReader(c)
		if authPass != "" {
			_, _ = fmt.Fprintf(c, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(authPass), authPass)
			_, _ = r.ReadString('\n')
		}
		nodeConns[addr] = c
		return c, r, nil
	}

	for i, move := range plan.Moves {
		// 1. Mark slot as IMPORTING on receiver
		recConn, recReader, err := getConn(move.ToAddr)
		if err == nil {
			_, _ = fmt.Fprintf(recConn, "*5\r\n$7\r\nCLUSTER\r\n$7\r\nSETSLOT\r\n$%d\r\n%d\r\n$9\r\nIMPORTING\r\n$%d\r\n%s\r\n",
				len(strconv.Itoa(int(move.Slot))), move.Slot, len(move.FromNodeID), move.FromNodeID)
			_, _ = recReader.ReadString('\n')
		}

		// 2. Mark slot as MIGRATING on donor
		donorConn, donorReader, err := getConn(move.FromAddr)
		if err == nil {
			_, _ = fmt.Fprintf(donorConn, "*5\r\n$7\r\nCLUSTER\r\n$7\r\nSETSLOT\r\n$%d\r\n%d\r\n$9\r\nMIGRATING\r\n$%d\r\n%s\r\n",
				len(strconv.Itoa(int(move.Slot))), move.Slot, len(move.ToNodeID), move.ToNodeID)
			_, _ = donorReader.ReadString('\n')
		}

		// 3. Migrate keys in this slot from donor to receiver (if any)
		if donorConn != nil && recConn != nil {
			migrateSlotKeys(donorConn, donorReader, recConn, recReader, move.Slot)
		}

		// 4. Finalize slot ownership to receiver across both nodes
		if recConn != nil {
			_, _ = fmt.Fprintf(recConn, "*5\r\n$7\r\nCLUSTER\r\n$7\r\nSETSLOT\r\n$%d\r\n%d\r\n$4\r\nNODE\r\n$%d\r\n%s\r\n",
				len(strconv.Itoa(int(move.Slot))), move.Slot, len(move.ToNodeID), move.ToNodeID)
			_, _ = recReader.ReadString('\n')
		}
		if donorConn != nil {
			_, _ = fmt.Fprintf(donorConn, "*5\r\n$7\r\nCLUSTER\r\n$7\r\nSETSLOT\r\n$%d\r\n%d\r\n$4\r\nNODE\r\n$%d\r\n%s\r\n",
				len(strconv.Itoa(int(move.Slot))), move.Slot, len(move.ToNodeID), move.ToNodeID)
			_, _ = donorReader.ReadString('\n')
		}

		if progressCb != nil {
			progressCb(i+1, len(plan.Moves), move)
		}
	}

	return nil
}

func migrateSlotKeys(donorConn net.Conn, donorReader *bufio.Reader, recConn net.Conn, recReader *bufio.Reader, slot uint16) {
	// Query donor for keys in slot
	_, _ = fmt.Fprintf(donorConn, "*4\r\n$7\r\nCLUSTER\r\n$15\r\nGETKEYSINSLOT\r\n$%d\r\n%d\r\n$3\r\n100\r\n",
		len(strconv.Itoa(int(slot))), slot)
	line, err := donorReader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "*") {
		return
	}

	count, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	for i := 0; i < count; i++ {
		// Read bulk string key
		lenLine, _ := donorReader.ReadString('\n')
		if strings.HasPrefix(lenLine, "$") {
			kLen, _ := strconv.Atoi(strings.TrimSpace(lenLine[1:]))
			kBuf := make([]byte, kLen)
			_, _ = donorReader.Read(kBuf)
			_, _ = donorReader.ReadString('\n') // CRLF
			key := string(kBuf)

			// Fetch value from donor and write to receiver
			_, _ = fmt.Fprintf(donorConn, "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
			vLine, _ := donorReader.ReadString('\n')
			if strings.HasPrefix(vLine, "$") {
				vLen, _ := strconv.Atoi(strings.TrimSpace(vLine[1:]))
				if vLen >= 0 {
					vBuf := make([]byte, vLen)
					_, _ = donorReader.Read(vBuf)
					_, _ = donorReader.ReadString('\n') // CRLF
					val := string(vBuf)

					// SET on receiver
					_, _ = fmt.Fprintf(recConn, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
					_, _ = recReader.ReadString('\n')

					// DEL on donor
					_, _ = fmt.Fprintf(donorConn, "*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
					_, _ = donorReader.ReadString('\n')
				}
			}
		}
	}
}
