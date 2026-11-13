package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The repro on a live tenant (resolve-hostnames yes, announce-hostnames no): the operator points
// SENTINEL MONITOR at the master's headless hostname, Sentinel resolves it once and reports only
// the IP, and the skip check compared the two strings. They never matched, so every reconcile
// issued SENTINEL REMOVE + MONITOR on a healthy sentinel, discarding its known replicas and
// sentinels and resetting its config epoch.
func TestSentinelMonitorComparesAHostnameTargetByItsAddress(t *testing.T) {
	tests := []struct {
		name         string
		monitoredIP  string
		target       string
		wantErr      bool
		wantCommands []string // SENTINEL REMOVE / MONITOR commands issued, in order
	}{
		{
			name:         "sentinel already on the address the hostname resolves to is left alone",
			monitoredIP:  "127.0.0.1",
			target:       "localhost",
			wantCommands: nil,
		},
		{
			name:        "sentinel left on the IP of a master that moved is re-pointed",
			monitoredIP: "10.224.5.56",
			target:      "localhost",
			wantCommands: []string{
				"SENTINEL REMOVE MYMASTER",
				"SENTINEL MONITOR MYMASTER LOCALHOST 6379 2",
			},
		},
		{
			name:         "an unresolvable target leaves the sentinel monitoring what it has",
			monitoredIP:  "10.224.5.56",
			target:       "no-such-master.invalid",
			wantErr:      true,
			wantCommands: nil,
		},
		{
			name:         "an IP target equal to the monitored IP is left alone",
			monitoredIP:  "10.224.1.146",
			target:       "10.224.1.146",
			wantCommands: nil,
		},
		{
			name:        "an IP target different from the monitored IP is re-pointed",
			monitoredIP: "10.224.5.56",
			target:      "10.224.1.146",
			wantCommands: []string{
				"SENTINEL REMOVE MYMASTER",
				"SENTINEL MONITOR MYMASTER 10.224.1.146 6379 2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeMonitorSentinel{master: map[string]string{
				"name": "mymaster", "ip": tt.monitoredIP, "port": "6379", "flags": "master",
			}}
			host, port := f.start(t)
			svc := NewClient().Connect(&ConnectionInfo{Host: host, Port: port})

			err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: tt.target, Port: "6379"}, "mymaster", "2")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCommands, f.mutations())
		})
	}
}

// Every SENTINEL SET rewrites the sentinel's config file, and the reconcile now repeats every
// minute, so a value the sentinel already holds must not be sent again. Under RESP3 Sentinel
// answers SENTINEL MASTER with a map, which a slice read takes only half of.
func TestSentinelSetSkipsAValueTheSentinelAlreadyHolds(t *testing.T) {
	for _, resp3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("resp3=%v", resp3), func(t *testing.T) {
			f := &fakeMonitorSentinel{resp3: resp3, master: map[string]string{
				"name": "mymaster", "ip": "10.0.0.1", "port": "6379", "down-after-milliseconds": "3000", "parallel-syncs": "1",
			}}
			host, port := f.start(t)
			svc := NewClient().Connect(&ConnectionInfo{Host: host, Port: port})

			require.NoError(t, svc.SentinelSet(context.Background(), "mymaster", "down-after-milliseconds", "3000"))
			require.NoError(t, svc.SentinelSet(context.Background(), "mymaster", "parallel-syncs", "2"))
			assert.Equal(t, []string{"SENTINEL SET MYMASTER PARALLEL-SYNCS 2"}, f.mutations())
		})
	}
}

// The healer resolves a hostname master at most once per pass and hands the lookup in. It is
// called only when a sentinel reports the master as an IP; a failed lookup leaves the sentinel
// as it is and is reported as ErrMonitorTargetUnresolved.
func TestSentinelMonitorUsesTheResolverItIsGiven(t *testing.T) {
	const target = "valkey-2.valkey-headless.ns.svc.no-such-domain.invalid"
	tests := []struct {
		name        string
		reported    string
		resolve     func(context.Context) ([]string, error)
		wantErr     error
		wantLookups int
	}{
		{
			name:        "sentinel reports the IP the resolver gives: left alone",
			reported:    "10.224.1.146",
			resolve:     func(context.Context) ([]string, error) { return []string{"10.224.1.146"}, nil },
			wantLookups: 1,
		},
		{
			name:        "sentinel reports the hostname: no lookup",
			reported:    target,
			resolve:     func(context.Context) ([]string, error) { return nil, errors.New("must not be called") },
			wantLookups: 0,
		},
		{
			name:     "lookup fails: left alone, unresolved",
			reported: "10.224.5.56",
			resolve: func(context.Context) ([]string, error) {
				return nil, fmt.Errorf("%w: %s: no such host", ErrMonitorTargetUnresolved, target)
			},
			wantErr:     ErrMonitorTargetUnresolved,
			wantLookups: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeMonitorSentinel{master: map[string]string{"name": "mymaster", "ip": tt.reported, "port": "6379"}}
			host, port := f.start(t)
			svc := NewClient().Connect(&ConnectionInfo{Host: host, Port: port})
			lookups := 0
			resolve := func(ctx context.Context) ([]string, error) { lookups++; return tt.resolve(ctx) }

			err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: target, Port: "6379", Resolve: resolve}, "mymaster", "2")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantLookups, lookups)
			assert.Empty(t, f.mutations())
		})
	}
}

func TestResolveHostFailureIsUnresolved(t *testing.T) {
	orig := lookupHost
	t.Cleanup(func() { lookupHost = orig })
	lookupHost = func(context.Context, string) ([]string, error) { return nil, errors.New("no such host") }
	_, err := ResolveHost(context.Background(), "valkey-1.valkey-headless.ns.svc.cluster.local")
	require.ErrorIs(t, err, ErrMonitorTargetUnresolved)
}

func TestResolveHostIsAbsoluteAndBounded(t *testing.T) {
	orig := lookupHost
	t.Cleanup(func() { lookupHost = orig })
	var gotName string
	var hasDeadline bool
	lookupHost = func(ctx context.Context, name string) ([]string, error) {
		gotName = name
		_, hasDeadline = ctx.Deadline()
		return []string{"10.0.0.7"}, nil
	}

	addrs, err := ResolveHost(context.Background(), "valkey-2.valkey-headless.ns.svc.cluster.local")
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.7"}, addrs)
	assert.Equal(t, "valkey-2.valkey-headless.ns.svc.cluster.local.", gotName, "an FQDN skips the search domains")
	assert.True(t, hasDeadline, "the lookup is bounded")
}

func TestSentinelPeersParsesBothProtocols(t *testing.T) {
	for _, resp3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("resp3=%v", resp3), func(t *testing.T) {
			f := &fakeMonitorSentinel{resp3: resp3, master: map[string]string{"name": "mymaster"}, replicas: []map[string]string{
				{"name": "10.0.0.2:6379", "ip": "10.0.0.2", "port": "6379", "flags": "slave"},
				{"name": "10.0.0.9:6379", "ip": "10.0.0.9", "port": "6379", "flags": "s_down,slave,disconnected"},
			}}
			host, port := f.start(t)
			got, err := NewClient().Connect(&ConnectionInfo{Host: host, Port: port}).SentinelPeers(context.Background(), "mymaster")
			require.NoError(t, err)
			assert.Equal(t, []SentinelPeer{
				{IP: "10.0.0.2", Port: "6379", Flags: "slave"},
				{IP: "10.0.0.9", Port: "6379", Flags: "s_down,slave,disconnected"},
			}, got.Replicas)
			assert.Empty(t, got.Sentinels)
		})
	}
}

// fakeMonitorSentinel is a minimal RESP2 sentinel: it answers SENTINEL MASTER with the
// configured fields and records every SENTINEL REMOVE / MONITOR / SET it receives.
type fakeMonitorSentinel struct {
	master   map[string]string
	replicas []map[string]string
	resp3    bool // accept HELLO 3 and answer instance replies as RESP3 maps
	mu       sync.Mutex
	commands []string
	wg       sync.WaitGroup
}

func (f *fakeMonitorSentinel) start(t *testing.T) (string, string) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				f.serve(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		f.wg.Wait()
	})
	host, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	return host, port
}

func (f *fakeMonitorSentinel) mutations() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.commands {
		if strings.HasPrefix(c, "SENTINEL REMOVE") || strings.HasPrefix(c, "SENTINEL MONITOR") || strings.HasPrefix(c, "SENTINEL SET") {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeMonitorSentinel) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		args, err := readRESPCommand(r)
		if err != nil {
			return
		}
		cmd := strings.ToUpper(strings.Join(args, " "))
		f.mu.Lock()
		f.commands = append(f.commands, cmd)
		f.mu.Unlock()
		var out string
		switch {
		case strings.HasPrefix(cmd, "HELLO"):
			if f.resp3 && strings.HasPrefix(cmd, "HELLO 3") {
				out = f.instance(map[string]string{"server": "redis", "proto": "3"})
			} else {
				out = "-ERR unknown command 'HELLO'\r\n"
			}
		case strings.HasPrefix(cmd, "SENTINEL MASTER"):
			out = f.instance(f.master)
		case strings.HasPrefix(cmd, "SENTINEL REPLICAS"):
			out = fmt.Sprintf("*%d\r\n", len(f.replicas))
			for _, r := range f.replicas {
				out += f.instance(r)
			}
		case strings.HasPrefix(cmd, "SENTINEL SENTINELS"):
			out = "*0\r\n"
		default:
			out = "+OK\r\n"
		}
		if _, err := conn.Write([]byte(out)); err != nil {
			return
		}
	}
}

// instance encodes one sentinel instance reply: a flat array under RESP2, a map under RESP3.
func (f *fakeMonitorSentinel) instance(fields map[string]string) string {
	out := fmt.Sprintf("*%d\r\n", 2*len(fields))
	if f.resp3 {
		out = fmt.Sprintf("%%%d\r\n", len(fields))
	}
	for k, v := range fields {
		out += fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
	}
	return out
}

func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("not an array: %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimRight(hdr, "\r\n")[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}
