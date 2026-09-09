package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spiridonov/deadhorse"
)

// TextServer serves DHP/1, DeadHorse's line-oriented text protocol, over any
// Throttler. Each line is one newline-terminated command; a connection may
// pipeline many requests without waiting for a response to each. TextServer
// is a single, independent process: it has no notion of shards, other
// TextServers, or a cluster -- routing a key to one of several DeadHorse
// processes is entirely a client-side concern (see the client package).
type TextServer struct {
	throttler   deadhorse.Throttler
	maxLineSize int
	startedAt   time.Time

	// listenerMu guards listener, which is written by ListenAndServe and
	// read by Close -- two methods meant to be called from different
	// goroutines (Close is how you make a blocking ListenAndServe return).
	listenerMu sync.Mutex
	listener   net.Listener
}

const (
	protocolVersion    = "1"
	defaultMaxLineSize = 64 * 1024
)

var errLineTooLong = errors.New("line too long")

// NewTextServer builds a TextServer over throttler. maxLineSize bounds how
// long a single protocol line may be before the connection is dropped (see
// readLine); zero or negative falls back to defaultMaxLineSize.
func NewTextServer(throttler deadhorse.Throttler, maxLineSize int) *TextServer {
	if maxLineSize <= 0 {
		maxLineSize = defaultMaxLineSize
	}
	return &TextServer{
		throttler:   throttler,
		maxLineSize: maxLineSize,
		startedAt:   time.Now(),
	}
}

// ListenAndServe accepts connections on addr until the listener is closed
// (via Close), serving each on its own goroutine.
func (s *TextServer) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listenerMu.Lock()
	s.listener = lis
	s.listenerMu.Unlock()

	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *TextServer) Close() error {
	s.listenerMu.Lock()
	lis := s.listener
	s.listenerMu.Unlock()

	if lis == nil {
		return nil
	}
	return lis.Close()
}

func (s *TextServer) handleConn(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReaderSize(conn, 4096)
	w := bufio.NewWriter(conn)

	for {
		line, err := readLine(r, s.maxLineSize)
		if errors.Is(err, errLineTooLong) {
			w.WriteString("ERROR line too long\n")
			w.Flush()
			return
		}
		if err != nil {
			return
		}
		if strings.TrimSpace(line) == "" {
			continue
		}

		resp, closeConn := s.dispatch(line)
		if resp != "" {
			w.WriteString(resp)
			w.WriteByte('\n')
		}
		if err := w.Flush(); err != nil {
			return
		}
		if closeConn {
			return
		}
	}
}

// readLine reads one line terminated by '\n' (a trailing '\r' is trimmed),
// bounded to maxSize bytes total so a client that never sends '\n' can't
// grow the server's memory without bound.
func readLine(r *bufio.Reader, maxSize int) (string, error) {
	var buf []byte
	for {
		frag, err := r.ReadSlice('\n')
		buf = append(buf, frag...)
		if len(buf) > maxSize {
			return "", errLineTooLong
		}
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return "", err
	}
	line := strings.TrimSuffix(string(buf), "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

func (s *TextServer) dispatch(line string) (response string, closeConn bool) {
	cmd, rest, _ := strings.Cut(line, " ")
	switch cmd {
	case "HELLO":
		return s.handleHello(rest), false
	case "THROTTLE":
		return s.handleThrottle(rest), false
	case "PING":
		return "PONG", false
	case "STATS":
		return s.handleStats(), false
	case "QUIT":
		return "", true
	default:
		return "ERROR unknown command", false
	}
}

func (s *TextServer) handleHello(rest string) string {
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "ERROR missing version"
	}
	if fields[0] != protocolVersion {
		return "ERROR unsupported version"
	}
	return "OK " + protocolVersion
}

func (s *TextServer) handleStats() string {
	keys := 0
	if kc, ok := s.throttler.(interface{ keyCountEstimate() int }); ok {
		keys = kc.keyCountEstimate()
	}
	uptime := int64(time.Since(s.startedAt).Seconds())
	return fmt.Sprintf("STATS %d %d", uptime, keys)
}

func (s *TextServer) handleThrottle(rest string) string {
	tokens := strings.Fields(rest)

	entries := make([]deadhorse.RequestEntry, 0, len(tokens))
	entryOK := make([]bool, len(tokens))
	entryIdx := make([]int, 0, len(tokens))
	for i, tok := range tokens {
		e, ok := parseEntry(tok)
		entryOK[i] = ok
		if ok {
			entries = append(entries, e)
			entryIdx = append(entryIdx, i)
		}
	}

	results := make([]string, len(tokens))
	if len(entries) > 0 {
		responses, err := s.throttler.Throttle(context.Background(), entries)
		if responses == nil {
			msg := "throttle failed"
			if err != nil {
				msg = err.Error()
			}
			return "ERROR " + msg
		}
		for j, resp := range responses {
			results[entryIdx[j]] = formatResult(resp)
		}
	}
	for i, ok := range entryOK {
		if !ok {
			results[i] = "ERR"
		}
	}

	if len(results) == 0 {
		return "RESULT"
	}
	return "RESULT " + strings.Join(results, " ")
}

func parseEntry(tok string) (deadhorse.RequestEntry, bool) {
	parts := strings.Split(tok, "|")
	if len(parts) != 5 {
		return deadhorse.RequestEntry{}, false
	}
	key := parts[0]
	if key == "" {
		return deadhorse.RequestEntry{}, false
	}
	capacity, err1 := strconv.ParseInt(parts[1], 10, 64)
	emissionInterval, err2 := strconv.ParseInt(parts[2], 10, 64)
	cost, err3 := strconv.ParseInt(parts[3], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || capacity < 0 || emissionInterval < 0 || cost < 0 {
		return deadhorse.RequestEntry{}, false
	}
	var peek bool
	switch parts[4] {
	case "R":
		peek = false
	case "P":
		peek = true
	default:
		return deadhorse.RequestEntry{}, false
	}
	return deadhorse.RequestEntry{
		Key: key,
		Limit: deadhorse.Limit{
			Capacity:         capacity,
			EmissionInterval: time.Duration(emissionInterval),
		},
		Cost: cost,
		Peek: peek,
	}, true
}

// formatResult collapses an errored entry to the bare ERR token, same as a
// parse failure -- the built-in InMemoryThrottler never sets Err, but
// TextServer works over any Throttler, and a custom one might.
func formatResult(r deadhorse.ResponseEntry) string {
	if r.Err != nil {
		return "ERR"
	}
	throttled := "0"
	if r.Throttled {
		throttled = "1"
	}
	return fmt.Sprintf("%s|%s|%d|%d", r.Key, throttled, r.Remaining, int64(r.RetryAfter))
}
