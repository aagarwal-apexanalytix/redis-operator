package bootstrap

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A restarted sentinel bootstrapped from whatever master its peers reported, with no check
// that it was still a master. A peer still holding a master that has moved (its pod gone, or
// its IP reused by a replica) would hand the new sentinel a dead master.
func TestPeerLookupDiscover_DropsAReportedMasterThatIsNotLive(t *testing.T) {
	dead := monitoredMaster{host: "10.0.1.10", port: "6379", configEpoch: 5}
	live := monitoredMaster{host: "10.0.1.11", port: "6379", configEpoch: 4}

	tests := []struct {
		name    string
		peers   map[string]monitoredMaster
		live    map[string]bool // master address -> answers ROLE master
		want    monitoredMaster
		wantErr error
	}{
		{
			name:  "a live master at a lower epoch wins over a dead one at a higher epoch",
			peers: map[string]monitoredMaster{"10.0.0.1": dead, "10.0.0.2": live},
			live:  map[string]bool{live.address(): true},
			want:  live,
		},
		{
			name:    "no reported master is live: fall back to the placeholder",
			peers:   map[string]monitoredMaster{"10.0.0.1": dead, "10.0.0.2": dead},
			live:    map[string]bool{},
			wantErr: errNoUsableAnswer,
		},
		{
			name:  "a live master is kept",
			peers: map[string]monitoredMaster{"10.0.0.1": live, "10.0.0.2": live},
			live:  map[string]bool{live.address(): true},
			want:  live,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resolved []string
			for addr := range tt.peers {
				resolved = append(resolved, addr)
			}
			verified := map[string]int{}
			l := peerLookup{
				service: "valkey-sentinel-headless.ns.svc.cluster.local",
				port:    "26379",
				resolve: func(context.Context, string) ([]string, error) { return resolved, nil },
				query: func(_ context.Context, addr string) (monitoredMaster, error) {
					host, _, _ := net.SplitHostPort(addr)
					return tt.peers[host], nil
				},
				verify: func(_ context.Context, addr string) error {
					verified[addr]++
					if tt.live[addr] {
						return nil
					}
					return errNotMaster
				},
			}

			got, err := l.discover(context.Background())
			for addr, n := range verified {
				assert.Equal(t, 1, n, "each distinct master is asked once: %s", addr)
			}
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPeerLookup_VerifiesTheMasterOverTCP drives discover with the production sentinel query and
// the production ROLE check: the peer reports a master that now answers as a replica.
func TestPeerLookup_VerifiesTheMasterOverTCP(t *testing.T) {
	for _, tc := range []struct {
		role    string
		wantErr error
	}{
		{role: "master"},
		{role: "slave", wantErr: errNoUsableAnswer},
	} {
		t.Run(tc.role, func(t *testing.T) {
			masterAddr := startFakeRoleServer(t, tc.role)
			mHost, mPort, err := net.SplitHostPort(masterAddr)
			require.NoError(t, err)
			peer := startFakeSentinel(t, &fakeSentinel{reply: map[string]string{"ip": mHost, "port": mPort, "config-epoch": "1"}})

			l := peerLookup{
				service: "valkey-sentinel-headless.ns.svc.cluster.local",
				port:    "26379",
				resolve: func(context.Context, string) ([]string, error) { return []string{"peer"}, nil },
				verify:  queryMasterRole("", 300*time.Millisecond),
			}
			q := querySentinelMaster("", "mymaster", 300*time.Millisecond)
			l.query = func(ctx context.Context, _ string) (monitoredMaster, error) { return q(ctx, peer) }

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := l.discover(ctx)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.Contains(t, err.Error(), "slave")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, mHost, got.host)
		})
	}
}

func TestQueryMasterRole_UnreachableMasterIsAnError(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	err = queryMasterRole("", time.Second)(context.Background(), addr)
	require.Error(t, err)
	assert.False(t, errors.Is(err, errNotMaster), "a dial failure is not a ROLE answer")
}

// startFakeRoleServer is a minimal RESP2 redis that answers ROLE with the given role.
func startFakeRoleServer(t *testing.T, role string) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				r := bufio.NewReader(conn)
				for {
					args, err := readRESPArray(r)
					if err != nil {
						return
					}
					var out string
					switch strings.ToUpper(args[0]) {
					case "HELLO":
						out = "-ERR unknown command 'HELLO'\r\n"
					case "ROLE":
						if role == "master" {
							out = "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"
						} else {
							out = "*5\r\n$5\r\nslave\r\n$8\r\n10.0.1.9\r\n:6379\r\n$9\r\nconnected\r\n:0\r\n"
						}
					default:
						out = "+OK\r\n"
					}
					if _, err := conn.Write([]byte(out)); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln.Addr().String()
}
