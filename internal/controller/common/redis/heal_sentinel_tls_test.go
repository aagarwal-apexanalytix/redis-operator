package redis

import (
	"context"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

// TestSentinelHealerDialsUnreadyTLSSentinelByIP pins that the sentinel repair path reaches a
// sentinel that is Running but NOT Ready, with TLS on. The master-aware readiness probe keeps a
// placeholder sentinel unready until SENTINEL MONITOR runs, and cluster DNS does not publish an
// unready pod's headless-service name, so dialing that name would deadlock the repair.
func TestSentinelHealerDialsUnreadyTLSSentinelByIP(t *testing.T) {
	labels := map[string]string{"app": "redis-sentinel-sentinel"}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-sentinel-sentinel", Namespace: "ns"},
		Spec:       appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{MatchLabels: labels}},
	}
	unready := newLabeledRedisPod("redis-sentinel-sentinel-0", labels, "10.0.0.20", corev1.PodRunning, false)
	unready.Namespace = "ns"

	rs := &rsvb2.RedisSentinel{ObjectMeta: metav1.ObjectMeta{Name: "redis-sentinel", Namespace: "ns"}}
	rs.Spec.TLS = &commonapi.TLSConfig{Secret: corev1.SecretVolumeSource{SecretName: "redis-tls-cert"}}
	rs.Spec.RedisSentinelConfig = &rsvb2.RedisSentinelConfig{}
	rs.Spec.RedisSentinelConfig.MasterGroupName = "mymaster"
	rs.Spec.RedisSentinelConfig.Quorum = "1"
	rs.Spec.RedisSentinelConfig.DownAfterMilliseconds = "5000"

	for name, call := range map[string]func(h *healer) error{
		"SentinelMonitor": func(h *healer) error { return h.SentinelMonitor(context.Background(), rs, "10.0.0.5") },
		"SentinelSet":     func(h *healer) error { return h.SentinelSet(context.Background(), rs, "10.0.0.5") },
		"SentinelReset":   func(h *healer) error { return h.SentinelReset(context.Background(), rs) },
	} {
		t.Run(name, func(t *testing.T) {
			redisClient := &fakeRedisClient{}
			h := &healer{k8s: k8sfake.NewSimpleClientset(sts, unready), redis: redisClient}
			require.NoError(t, call(h))
			require.NotEmpty(t, redisClient.connectHosts)
			for _, host := range redisClient.connectHosts {
				assert.Equal(t, "10.0.0.20", host, "an unready sentinel's DNS name does not resolve; dial its IP")
			}
		})
	}
}
