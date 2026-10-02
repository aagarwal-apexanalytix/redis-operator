package bootstrap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain keeps every GenerateConfig test in this package off the network: by default the
// peer lookup finds no peers, which is the behaviour these tests had before the lookup
// existed. Tests that exercise the lookup install their own stub.
func TestMain(m *testing.M) {
	discoverPeerMaster = func(string, string, string, bool) (monitoredMaster, error) {
		return monitoredMaster{}, errNoPeers
	}
	os.Exit(m.Run())
}

// peerAnswer is what a stubbed peer sentinel returns.
type peerAnswer struct {
	master monitoredMaster
	err    error
}

func TestPeerLookupDiscover(t *testing.T) {
	knownMaster := monitoredMaster{host: "10.0.1.10", port: "6379"}
	placeholder := monitoredMaster{host: "0.0.0.0", port: "6379"}
	unreachable := peerAnswer{err: errors.New("dial tcp: i/o timeout")}

	tests := []struct {
		name          string
		service       string
		resolveErr    error
		peers         map[string]peerAnswer // resolved address -> answer
		allowHostname bool
		want          monitoredMaster
		wantErr       error
	}{
		{
			name:    "no peer service name (pod has no FQDN)",
			service: "",
			wantErr: errNoPeerService,
		},
		{
			name:       "DNS lookup of the headless service fails",
			service:    "valkey-sentinel-headless.ns.svc.cluster.local",
			resolveErr: errors.New("no such host"),
			wantErr:    errNoPeers,
		},
		{
			name:    "no peers: first sentinel of a new deployment",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers:   map[string]peerAnswer{},
			wantErr: errNoPeers,
		},
		{
			name:    "all peers on the placeholder: every sentinel restarted at once",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {master: placeholder},
				"10.0.0.2": {master: placeholder},
			},
			wantErr: errNoUsableAnswer,
		},
		{
			name:    "every peer unreachable (timeouts)",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": unreachable,
				"10.0.0.2": unreachable,
			},
			wantErr: errNoUsableAnswer,
		},
		{
			name:    "one peer unreachable, one knows the master",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": unreachable,
				"10.0.0.2": {master: knownMaster},
			},
			want: knownMaster,
		},
		{
			name:    "one peer on the placeholder, one knows the master",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {master: placeholder},
				"10.0.0.2": {master: knownMaster},
			},
			want: knownMaster,
		},
		{
			name:    "peer reports a sentinel that does not monitor the group",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {err: errors.New("ERR No such master with that name")},
			},
			wantErr: errNoUsableAnswer,
		},
		{
			name:    "the newest failover wins over a stale majority",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {master: monitoredMaster{host: "10.0.1.11", port: "6379", configEpoch: 3}},
				"10.0.0.2": {master: monitoredMaster{host: "10.0.1.11", port: "6379", configEpoch: 3}},
				"10.0.0.3": {master: monitoredMaster{host: "10.0.1.12", port: "6379", configEpoch: 4}},
			},
			want: monitoredMaster{host: "10.0.1.12", port: "6379", configEpoch: 4},
		},
		{
			name:    "same epoch: the most-reported master wins",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {master: monitoredMaster{host: "10.0.1.11", port: "6379"}},
				"10.0.0.2": {master: monitoredMaster{host: "10.0.1.11", port: "6379"}},
				"10.0.0.3": {master: monitoredMaster{host: "10.0.1.12", port: "6379"}},
			},
			want: monitoredMaster{host: "10.0.1.11", port: "6379"},
		},
		{
			name:    "same epoch, split vote: ambiguous, leave it to the operator",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {master: monitoredMaster{host: "10.0.1.11", port: "6379"}},
				"10.0.0.2": {master: monitoredMaster{host: "10.0.1.12", port: "6379"}},
			},
			wantErr: errAmbiguousAnswer,
		},
		{
			name:    "hostname master rejected without resolve-hostnames",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {master: monitoredMaster{host: "valkey-0.valkey-headless.ns.svc.cluster.local", port: "6379"}},
			},
			wantErr: errNoUsableAnswer,
		},
		{
			name:    "hostname master accepted with resolve-hostnames",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {master: monitoredMaster{host: "valkey-0.valkey-headless.ns.svc.cluster.local", port: "6379"}},
			},
			allowHostname: true,
			want:          monitoredMaster{host: "valkey-0.valkey-headless.ns.svc.cluster.local", port: "6379"},
		},
		{
			name:    "unspecified IPv6 address and malformed ports are unusable",
			service: "valkey-sentinel-headless.ns.svc.cluster.local",
			peers: map[string]peerAnswer{
				"10.0.0.1": {master: monitoredMaster{host: "::", port: "6379"}},
				"10.0.0.2": {master: monitoredMaster{host: "10.0.1.11", port: "not-a-port"}},
				"10.0.0.3": {master: monitoredMaster{host: "10.0.1.11", port: "0"}},
				"10.0.0.4": {master: monitoredMaster{host: "", port: "6379"}},
			},
			wantErr: errNoUsableAnswer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resolved []string
			for addr := range tt.peers {
				resolved = append(resolved, addr)
			}
			var queried []string
			l := peerLookup{
				service:       tt.service,
				port:          "26379",
				allowHostname: tt.allowHostname,
				resolve: func(_ context.Context, host string) ([]string, error) {
					assert.Equal(t, tt.service, host)
					return resolved, tt.resolveErr
				},
				query: func(_ context.Context, addr string) (monitoredMaster, error) {
					queried = append(queried, addr)
					host, port, err := net.SplitHostPort(addr)
					require.NoError(t, err)
					assert.Equal(t, "26379", port, "peers must be asked on the sentinel port")
					a := tt.peers[host]
					return a.master, a.err
				},
			}

			got, err := l.discover(context.Background())
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Len(t, queried, len(tt.peers), "every resolved peer is asked")
		})
	}
}

func TestPeerLookupDiscover_StopsWhenBudgetIsSpent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var queried int
	l := peerLookup{
		service: "valkey-sentinel-headless.ns.svc.cluster.local",
		port:    "26379",
		resolve: func(context.Context, string) ([]string, error) {
			return []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, nil
		},
		query: func(context.Context, string) (monitoredMaster, error) {
			queried++
			cancel() // the first peer used up the whole budget
			return monitoredMaster{}, context.Canceled
		},
	}
	_, err := l.discover(ctx)
	require.ErrorIs(t, err, errNoUsableAnswer)
	assert.Equal(t, 1, queried, "no peer is asked once the lookup budget is spent")
}

func TestPeerLookupDiscover_IPv6PeerAddress(t *testing.T) {
	var asked string
	l := peerLookup{
		service: "valkey-sentinel-headless.ns.svc.cluster.local",
		port:    "26379",
		resolve: func(context.Context, string) ([]string, error) { return []string{"fd00::5"}, nil },
		query: func(_ context.Context, addr string) (monitoredMaster, error) {
			asked = addr
			return monitoredMaster{host: "fd00::7", port: "6379"}, nil
		},
	}
	got, err := l.discover(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "[fd00::5]:26379", asked)
	assert.Equal(t, "fd00::7", got.host)
}

func TestPeerServiceFromFQDN(t *testing.T) {
	tests := map[string]string{
		"valkey-sentinel-1.valkey-sentinel-headless.ns.svc.cluster.local":  "valkey-sentinel-headless.ns.svc.cluster.local",
		"valkey-sentinel-1.valkey-sentinel-headless.ns.svc.cluster.local.": "valkey-sentinel-headless.ns.svc.cluster.local",
		"valkey-sentinel-1":          "",
		"valkey-sentinel-1.headless": "",
		"":                           "",
	}
	for fqdn, want := range tests {
		assert.Equal(t, want, peerServiceFromFQDN(fqdn), fqdn)
	}
}

// fakeSentinel is a minimal RESP2 server standing in for a peer sentinel. It refuses HELLO
// (as a RESP2-only server does, which makes go-redis fall back to RESP2), answers AUTH, and
// answers SENTINEL MASTER with the configured reply. With hang set it accepts connections
// and never answers anything.
type fakeSentinel struct {
	ln       net.Listener
	hang     bool
	password string
	reply    map[string]string // SENTINEL MASTER fields; nil answers "no such master"
	wg       sync.WaitGroup
	mu       sync.Mutex
	commands []string
}

func startFakeSentinel(t *testing.T, f *fakeSentinel) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f.ln = ln
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
	return ln.Addr().String()
}

func (f *fakeSentinel) serve(conn net.Conn) {
	defer conn.Close()
	if f.hang {
		_, _ = io.Copy(io.Discard, conn) // returns when the client gives up and closes
		return
	}
	r := bufio.NewReader(conn)
	for {
		args, err := readRESPArray(r)
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
			out = "-ERR unknown command 'HELLO'\r\n"
		case strings.HasPrefix(cmd, "AUTH"):
			if len(args) == 2 && args[1] == f.password {
				out = "+OK\r\n"
			} else {
				out = "-WRONGPASS invalid username-password pair\r\n"
			}
		case strings.HasPrefix(cmd, "SENTINEL MASTER"):
			if f.password != "" && !f.authed() {
				out = "-NOAUTH Authentication required.\r\n"
			} else if f.reply == nil {
				out = "-ERR No such master with that name\r\n"
			} else {
				out = fmt.Sprintf("*%d\r\n", 2*len(f.reply))
				for k, v := range f.reply {
					out += fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
				}
			}
		default:
			out = "-ERR unknown command\r\n"
		}
		if _, err := conn.Write([]byte(out)); err != nil {
			return
		}
	}
}

func (f *fakeSentinel) authed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.commands {
		if strings.HasPrefix(c, "AUTH") {
			return true
		}
	}
	return false
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
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

func TestQuerySentinelMaster(t *testing.T) {
	t.Run("parses the monitored master and its config epoch", func(t *testing.T) {
		addr := startFakeSentinel(t, &fakeSentinel{reply: map[string]string{
			"name": "mymaster", "ip": "10.0.1.10", "port": "6379", "config-epoch": "7", "flags": "master",
		}})
		got, err := querySentinelMaster("", "mymaster", time.Second)(context.Background(), addr)
		require.NoError(t, err)
		assert.Equal(t, monitoredMaster{host: "10.0.1.10", port: "6379", configEpoch: 7}, got)
	})

	t.Run("authenticates with the sentinel password", func(t *testing.T) {
		f := &fakeSentinel{password: "s3cret", reply: map[string]string{"ip": "10.0.1.10", "port": "6379"}}
		addr := startFakeSentinel(t, f)
		got, err := querySentinelMaster("s3cret", "mymaster", time.Second)(context.Background(), addr)
		require.NoError(t, err)
		assert.Equal(t, "10.0.1.10", got.host)
	})

	t.Run("a peer that does not monitor the group is an error", func(t *testing.T) {
		addr := startFakeSentinel(t, &fakeSentinel{})
		_, err := querySentinelMaster("", "mymaster", time.Second)(context.Background(), addr)
		require.Error(t, err)
	})

	t.Run("a peer that accepts and never answers times out within the per-peer bound", func(t *testing.T) {
		addr := startFakeSentinel(t, &fakeSentinel{hang: true})
		start := time.Now()
		_, err := querySentinelMaster("", "mymaster", 300*time.Millisecond)(context.Background(), addr)
		require.Error(t, err)
		assert.Less(t, time.Since(start), 3*time.Second, "a hung peer must not stall the bootstrap")
	})

	t.Run("a closed port fails fast", func(t *testing.T) {
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := ln.Addr().String()
		require.NoError(t, ln.Close())
		start := time.Now()
		_, err = querySentinelMaster("", "mymaster", time.Second)(context.Background(), addr)
		require.Error(t, err)
		assert.Less(t, time.Since(start), 2*time.Second)
	})
}

// TestPeerLookup_EndToEndOverTCP drives discover with the production query against fake
// peers: one hung, one on the placeholder, one that knows the master.
func TestPeerLookup_EndToEndOverTCP(t *testing.T) {
	hung := startFakeSentinel(t, &fakeSentinel{hang: true})
	ph := startFakeSentinel(t, &fakeSentinel{reply: map[string]string{"ip": "0.0.0.0", "port": "6379"}})
	good := startFakeSentinel(t, &fakeSentinel{reply: map[string]string{"ip": "10.0.1.10", "port": "6379", "config-epoch": "0"}})

	// Every fake listens on its own port, so resolve to full addresses and ignore l.port.
	byHost := map[string]string{"hung": hung, "ph": ph, "good": good}
	l := peerLookup{
		service: "valkey-sentinel-headless.ns.svc.cluster.local",
		port:    "26379",
		resolve: func(context.Context, string) ([]string, error) { return []string{"hung", "ph", "good"}, nil },
	}
	q := querySentinelMaster("", "mymaster", 300*time.Millisecond)
	l.query = func(ctx context.Context, addr string) (monitoredMaster, error) {
		host, _, _ := net.SplitHostPort(addr)
		return q(ctx, byHost[host])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := l.discover(ctx)
	require.NoError(t, err)
	assert.Equal(t, monitoredMaster{host: "10.0.1.10", port: "6379"}, got)
}
