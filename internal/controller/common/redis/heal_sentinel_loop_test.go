package redis

import (
	"context"
	"errors"
	"sync"
	"testing"

	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	redisservice "github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

var (
	sentinelLoopLabels    = map[string]string{"app": "valkey-sentinel"}
	replicationLoopLabels = map[string]string{"app": "valkey"}
)

func sentinelLoopFixture(pods ...*corev1.Pod) (*rsvb2.RedisSentinel, []runtime.Object) {
	rs := &rsvb2.RedisSentinel{ObjectMeta: metav1.ObjectMeta{Name: "valkey", Namespace: "ns"}}
	rs.Spec.RedisSentinelConfig = &rsvb2.RedisSentinelConfig{}
	rs.Spec.RedisSentinelConfig.MasterGroupName = "mymaster"
	rs.Spec.RedisSentinelConfig.Quorum = "2"
	rs.Spec.RedisSentinelConfig.DownAfterMilliseconds = "3000"
	rs.Spec.RedisSentinelConfig.RedisReplicationName = "valkey"

	objs := []runtime.Object{
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "valkey-sentinel", Namespace: "ns"},
			Spec:       appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{MatchLabels: sentinelLoopLabels}},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "valkey", Namespace: "ns"},
			Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3)), Selector: &metav1.LabelSelector{MatchLabels: replicationLoopLabels}},
		},
	}
	for _, p := range pods {
		objs = append(objs, p)
	}
	return rs, objs
}

func sentinelLoopPod(name, ip string) *corev1.Pod {
	p := newLabeledRedisPod(name, sentinelLoopLabels, ip, corev1.PodRunning, true)
	p.Namespace = "ns"
	return p
}

// The live repro: one sentinel answers "no route to host" (its pod had just been deleted) and
// another "connection refused" (restarting). Each healer pass returned at the first of them, so
// the sentinels listed after it were never re-pointed at the new master.
func TestSentinelHealerContinuesPastAFailingSentinel(t *testing.T) {
	errNoRoute := errors.New("dial tcp 10.0.0.1:26379: connect: no route to host")
	errRefused := errors.New("dial tcp 10.0.0.2:26379: connect: connection refused")

	for name, call := range map[string]func(h *healer, rs *rsvb2.RedisSentinel) error{
		"SentinelMonitor": func(h *healer, rs *rsvb2.RedisSentinel) error {
			return h.SentinelMonitor(context.Background(), rs, "10.0.0.50")
		},
		"SentinelSet": func(h *healer, rs *rsvb2.RedisSentinel) error {
			return h.SentinelSet(context.Background(), rs, "10.0.0.50")
		},
		"SentinelReset": func(h *healer, rs *rsvb2.RedisSentinel) error {
			return h.SentinelReset(context.Background(), rs)
		},
	} {
		t.Run(name, func(t *testing.T) {
			rs, objs := sentinelLoopFixture(
				sentinelLoopPod("valkey-sentinel-0", "10.0.0.1"),
				sentinelLoopPod("valkey-sentinel-1", "10.0.0.2"),
				sentinelLoopPod("valkey-sentinel-2", "10.0.0.3"),
			)
			client := &sentinelCallRecorder{errByHost: map[string]error{
				"10.0.0.1": errNoRoute,
				"10.0.0.2": errRefused,
			}}
			h := &healer{k8s: k8sfake.NewSimpleClientset(objs...), redis: client}

			err := call(h, rs)

			require.Error(t, err)
			assert.ErrorIs(t, err, errNoRoute, "every failing sentinel is reported")
			assert.ErrorIs(t, err, errRefused, "every failing sentinel is reported")
			assert.Contains(t, client.succeeded(), "10.0.0.3", "the healthy sentinel is still reached")
			var podsErr *SentinelPodsError
			require.ErrorAs(t, err, &podsErr)
			assert.Equal(t, 2, podsErr.Failed)
			assert.Equal(t, 3, podsErr.Total)
			assert.False(t, podsErr.AllFailed())
		})
	}
}

// A sentinel pod that has no IP yet was dialed at ":26379", the operator's own loopback, and its
// "connection refused" aborted the pass. A Pending pod with an IP is still in its init
// containers, a terminating pod is about to be replaced, and an evicted pod keeps its PodIP but
// runs nothing; none of them is a failed sentinel.
func TestSentinelHealerSkipsSentinelsThatCannotBeDialed(t *testing.T) {
	pending := sentinelLoopPod("valkey-sentinel-1", "")
	pending.Status.Phase = corev1.PodPending
	terminating := sentinelLoopPod("valkey-sentinel-2", "10.0.0.3")
	terminating.DeletionTimestamp = &metav1.Time{}
	terminating.Finalizers = []string{"test/hold"}
	evicted := sentinelLoopPod("valkey-sentinel-3", "10.0.0.4") // an evicted pod keeps its PodIP
	evicted.Status.Phase = corev1.PodFailed
	initializing := sentinelLoopPod("valkey-sentinel-4", "10.0.0.5") // Pending with an IP: still in init
	initializing.Status.Phase = corev1.PodPending

	for name, call := range map[string]func(h *healer, rs *rsvb2.RedisSentinel) error{
		"SentinelMonitor": func(h *healer, rs *rsvb2.RedisSentinel) error {
			return h.SentinelMonitor(context.Background(), rs, "10.0.0.50")
		},
		"SentinelSet": func(h *healer, rs *rsvb2.RedisSentinel) error {
			return h.SentinelSet(context.Background(), rs, "10.0.0.50")
		},
		"SentinelReset": func(h *healer, rs *rsvb2.RedisSentinel) error {
			return h.SentinelReset(context.Background(), rs)
		},
	} {
		t.Run(name, func(t *testing.T) {
			rs, objs := sentinelLoopFixture(sentinelLoopPod("valkey-sentinel-0", "10.0.0.1"), pending, terminating, evicted, initializing)
			client := &sentinelCallRecorder{}
			h := &healer{k8s: k8sfake.NewSimpleClientset(objs...), redis: client}

			require.NoError(t, call(h, rs))
			assert.Equal(t, []string{"10.0.0.1"}, client.connected())
		})
	}
}

// Staleness is decided by address. A count cap (rs.Spec.Size) made every sentinel look stale
// when other legitimate sentinels watch the same group (a second RedisSentinel, or an embedded
// one), and each was then reset on every pass.
func TestSentinelResetDecidesStalenessByAddress(t *testing.T) {
	others := []*corev1.Pod{
		// the replication's pods
		newLabeledRedisPod("valkey-0", replicationLoopLabels, "10.1.0.1", corev1.PodRunning, true),
		newLabeledRedisPod("valkey-1", replicationLoopLabels, "10.1.0.2", corev1.PodRunning, true),
		// another RedisSentinel's pods, watching the same group
		newLabeledRedisPod("other-sentinel-0", map[string]string{"app": "other"}, "10.0.0.8", corev1.PodRunning, true),
		newLabeledRedisPod("other-sentinel-1", map[string]string{"app": "other"}, "10.0.0.9", corev1.PodRunning, true),
		// an evicted replication pod keeps its old IP
		newLabeledRedisPod("valkey-2", replicationLoopLabels, "10.1.0.66", corev1.PodFailed, false),
		// an unrelated workload in the namespace that now holds a departed replica's IP
		newLabeledRedisPod("batch-job", map[string]string{"app": "batch"}, "10.1.0.70", corev1.PodRunning, true),
	}
	const fqdn = ".valkey-headless.ns.svc.cluster.local"
	replica := func(ip, flags string) redisservice.SentinelPeer {
		return redisservice.SentinelPeer{IP: ip, Port: "6379", Flags: flags}
	}
	sentinel := func(ip, flags string) redisservice.SentinelPeer {
		return redisservice.SentinelPeer{IP: ip, Port: "26379", Flags: flags}
	}
	tests := []struct {
		name      string
		peers     redisservice.SentinelPeersResult
		wantReset bool
	}{
		{
			name: "extra legitimate sentinels watching the same group: no reset",
			peers: redisservice.SentinelPeersResult{
				Replicas:  []redisservice.SentinelPeer{replica("10.1.0.1", "slave"), replica("10.1.0.2", "slave")},
				Sentinels: []redisservice.SentinelPeer{sentinel("10.0.0.2", "sentinel"), sentinel("10.0.0.3", "sentinel"), sentinel("10.0.0.8", "sentinel"), sentinel("10.0.0.9", "sentinel")},
			},
		},
		{
			name: "a replica left at the old IP of a moved pod, down: reset",
			peers: redisservice.SentinelPeersResult{
				Replicas: []redisservice.SentinelPeer{replica("10.1.0.1", "slave"), replica("10.1.0.66", "s_down,slave,disconnected")},
			},
			wantReset: true,
		},
		{
			name: "a sentinel left at the old IP of a replaced pod, disconnected: reset",
			peers: redisservice.SentinelPeersResult{
				Sentinels: []redisservice.SentinelPeer{sentinel("10.0.0.77", "sentinel,disconnected")},
			},
			wantReset: true,
		},
		{
			name: "an unknown IP the sentinel still reaches: no reset",
			peers: redisservice.SentinelPeersResult{
				Replicas: []redisservice.SentinelPeer{replica("10.1.0.99", "slave")},
			},
		},
		{
			name: "a down hostname entry of a pod scaled away (announce-hostnames yes): reset",
			peers: redisservice.SentinelPeersResult{
				Replicas: []redisservice.SentinelPeer{replica("valkey-0"+fqdn, "slave"), replica("valkey-4"+fqdn, "s_down,slave,disconnected")},
			},
			wantReset: true,
		},
		{
			name: "a down hostname entry of a live pod: no reset",
			peers: redisservice.SentinelPeersResult{
				Sentinels: []redisservice.SentinelPeer{sentinel("valkey-sentinel-0.valkey-sentinel-headless.ns.svc.cluster.local", "s_down,sentinel")},
			},
		},
		{
			name: "a down entry at an IP an unrelated pod now holds: reset",
			peers: redisservice.SentinelPeersResult{
				Replicas: []redisservice.SentinelPeer{replica("10.1.0.70", "s_down,slave,disconnected")},
			},
			wantReset: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, objs := sentinelLoopFixture(sentinelLoopPod("valkey-sentinel-0", "10.0.0.1"))
			rs.Spec.Size = ptr.To(int32(3))
			for _, p := range others {
				p := p.DeepCopy()
				p.Namespace = "ns"
				objs = append(objs, p)
			}
			client := &sentinelCallRecorder{peersByHost: map[string]redisservice.SentinelPeersResult{"10.0.0.1": tt.peers}}
			h := &healer{k8s: k8sfake.NewSimpleClientset(objs...), redis: client}

			require.NoError(t, h.SentinelReset(context.Background(), rs))
			if tt.wantReset {
				assert.Equal(t, []string{"10.0.0.1"}, client.resetHosts())
			} else {
				assert.Empty(t, client.resetHosts())
			}
		})
	}
}

// SENTINEL RESET discards the sentinel's peers and aborts a failover in progress; Redis advises
// resetting sentinels one at a time. Only one is reset per pass even when several are stale.
func TestSentinelResetResetsOneSentinelPerPass(t *testing.T) {
	rs, objs := sentinelLoopFixture(
		sentinelLoopPod("valkey-sentinel-0", "10.0.0.1"),
		sentinelLoopPod("valkey-sentinel-1", "10.0.0.2"),
		sentinelLoopPod("valkey-sentinel-2", "10.0.0.3"),
	)
	stale := redisservice.SentinelPeersResult{Replicas: []redisservice.SentinelPeer{{IP: "10.1.0.66", Port: "6379", Flags: "s_down,slave"}}}
	client := &sentinelCallRecorder{peersByHost: map[string]redisservice.SentinelPeersResult{
		"10.0.0.1": stale, "10.0.0.2": stale, "10.0.0.3": stale,
	}}
	h := &healer{k8s: k8sfake.NewSimpleClientset(objs...), redis: client}

	require.NoError(t, h.SentinelReset(context.Background(), rs))
	assert.Len(t, client.resetHosts(), 1)
}

// A hostname master is looked up at most once per pass, only when a sentinel reports the master
// as an IP. While the master pod is not ready its headless name may not resolve; that leaves the
// sentinels as they are for this pass instead of failing it (which backed off exponentially).
func TestSentinelMonitorResolvesTheMasterLazilyOncePerPass(t *testing.T) {
	tests := []struct {
		name         string
		reportsIP    bool
		lookupErr    error
		wantLookups  int
		wantMonitors int
	}{
		{name: "every sentinel reports an IP: one lookup for the pass", reportsIP: true, wantLookups: 1, wantMonitors: 3},
		{name: "every sentinel reports the hostname: no lookup", reportsIP: false, lookupErr: errors.New("NXDOMAIN"), wantLookups: 0, wantMonitors: 3},
		{name: "lookup fails: sentinels left as they are, pass not failed", reportsIP: true, lookupErr: errors.New("NXDOMAIN"), wantLookups: 1, wantMonitors: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, objs := sentinelLoopFixture(
				sentinelLoopPod("valkey-sentinel-0", "10.0.0.1"),
				sentinelLoopPod("valkey-sentinel-1", "10.0.0.2"),
				sentinelLoopPod("valkey-sentinel-2", "10.0.0.3"),
			)
			client := &sentinelCallRecorder{reportsIP: tt.reportsIP}
			lookups := 0
			h := &healer{
				k8s:   k8sfake.NewSimpleClientset(objs...),
				redis: client,
				resolveHost: func(context.Context, string) ([]string, error) {
					lookups++
					return []string{"10.1.0.2"}, tt.lookupErr
				},
			}

			require.NoError(t, h.SentinelMonitor(context.Background(), rs, "valkey-1.valkey-headless.ns.svc.cluster.local"))
			assert.Equal(t, tt.wantLookups, lookups)
			assert.Len(t, client.monitorTargets, tt.wantMonitors)
		})
	}
}

// sentinelCallRecorder is a redis client whose sentinel commands fail per host and are recorded.
type sentinelCallRecorder struct {
	errByHost   map[string]error
	peersByHost map[string]redisservice.SentinelPeersResult
	// reportsIP makes every sentinel report the master as an IP, so SentinelMonitor needs the
	// target's resolved addresses, as the real service does.
	reportsIP bool

	mu        sync.Mutex
	hosts     []string
	succeeds  []string
	resetsFor []string

	monitorTargets []redisservice.ConnectionInfo
}

func (c *sentinelCallRecorder) Connect(info *redisservice.ConnectionInfo) redisservice.Service {
	c.mu.Lock()
	c.hosts = append(c.hosts, info.Host)
	c.mu.Unlock()
	return &sentinelCallService{c: c, host: info.Host}
}

func (c *sentinelCallRecorder) connected() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return dedupe(c.hosts)
}

func (c *sentinelCallRecorder) succeeded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return dedupe(c.succeeds)
}

func (c *sentinelCallRecorder) resetHosts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return dedupe(c.resetsFor)
}

func (c *sentinelCallRecorder) call(host string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.errByHost[host]; err != nil {
		return err
	}
	c.succeeds = append(c.succeeds, host)
	return nil
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

type sentinelCallService struct {
	c    *sentinelCallRecorder
	host string
}

func (s *sentinelCallService) IsMaster(context.Context) (bool, error) { return false, nil }

func (s *sentinelCallService) GetAttachedReplicaCount(context.Context) (int, error) { return 0, nil }

func (s *sentinelCallService) SentinelMonitor(ctx context.Context, master *redisservice.ConnectionInfo, _, _ string) error {
	if s.c.reportsIP && master.Resolve != nil {
		if _, err := master.Resolve(ctx); err != nil {
			return err
		}
	}
	s.c.mu.Lock()
	s.c.monitorTargets = append(s.c.monitorTargets, *master)
	s.c.mu.Unlock()
	return s.c.call(s.host)
}

func (s *sentinelCallService) SentinelSet(context.Context, string, string, string) error {
	return s.c.call(s.host)
}

func (s *sentinelCallService) SentinelReset(context.Context, string) error {
	if err := s.c.call(s.host); err != nil {
		return err
	}
	s.c.mu.Lock()
	s.c.resetsFor = append(s.c.resetsFor, s.host)
	s.c.mu.Unlock()
	return nil
}

// GetInfoSentinel derives INFO sentinel counts from the configured peers.
func (s *sentinelCallService) GetInfoSentinel(context.Context) (*redisservice.InfoSentinelResult, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if err := s.c.errByHost[s.host]; err != nil {
		return nil, err
	}
	p := s.c.peersByHost[s.host]
	return &redisservice.InfoSentinelResult{Masters: []redisservice.SentinelMasterInfo{
		{Name: "mymaster", Status: "ok", Slaves: len(p.Replicas), Sentinels: len(p.Sentinels) + 1},
	}}, nil
}

func (s *sentinelCallService) SentinelPeers(context.Context, string) (*redisservice.SentinelPeersResult, error) {
	if err := s.c.call(s.host); err != nil {
		return nil, err
	}
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	p := s.c.peersByHost[s.host]
	return &p, nil
}

func (s *sentinelCallService) GetClusterInfo(context.Context) (*redisservice.ClusterStatus, error) {
	return &redisservice.ClusterStatus{}, nil
}
