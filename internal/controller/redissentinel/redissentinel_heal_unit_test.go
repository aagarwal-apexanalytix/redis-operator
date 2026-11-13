package redissentinel

import (
	"context"
	"errors"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	commonredis "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newSentinelReconcilerForHealTest(t *testing.T, healer *recordingHealer) (*RedisSentinelReconciler, *rsvb2.RedisSentinel) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, rrvb2.AddToScheme(scheme))
	require.NoError(t, rsvb2.AddToScheme(scheme))

	rr := &rrvb2.RedisReplication{ObjectMeta: metav1.ObjectMeta{Name: "valkey", Namespace: "ns"}}
	rr.Spec.Size = ptr.To(int32(3))
	rs := &rsvb2.RedisSentinel{ObjectMeta: metav1.ObjectMeta{Name: "valkey", Namespace: "ns"}}
	rs.Spec.Size = ptr.To(int32(3))
	rs.Spec.KubernetesConfig.Image = "quay.io/opstree/redis-sentinel:v8.6.2"
	rs.Spec.RedisSentinelConfig = &rsvb2.RedisSentinelConfig{}
	rs.Spec.RedisSentinelConfig.RedisReplicationName = "valkey"
	rs.Spec.RedisSentinelConfig.MasterGroupName = "mymaster"
	rs.Spec.RedisSentinelConfig.Quorum = "2"

	return &RedisSentinelReconciler{
		Client:    clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rr, rs).Build(),
		K8sClient: k8sfake.NewSimpleClientset(),
		Checker:   &masterChecker{master: "10.0.0.50"},
		Healer:    healer,
	}, rs
}

// A failing SENTINEL MONITOR returned before SentinelSet and SentinelReset ran, so the reachable
// sentinels never got the rest of the repair either. An API failure is still returned.
func TestReconcileSentinelRunsEveryStepWhenOneFails(t *testing.T) {
	errMonitor := errors.New("statefulsets.apps \"valkey-sentinel\" is forbidden")
	healer := &recordingHealer{monitorErr: errMonitor}
	r, rs := newSentinelReconcilerForHealTest(t, healer)

	_, err := r.reconcileSentinel(context.Background(), rs)

	require.ErrorIs(t, err, errMonitor)
	assert.Equal(t, []string{"monitor", "set", "reset"}, healer.calls)
}

// Sentinel stores the master as an IP, so a master pod that moves without a CR event leaves a
// sentinel on the old IP. Nothing requeued a healthy reconcile, so that sentinel stayed there.
func TestReconcileSentinelRequeuesAHealthyPass(t *testing.T) {
	healer := &recordingHealer{}
	r, rs := newSentinelReconcilerForHealTest(t, healer)

	result, err := r.reconcileSentinel(context.Background(), rs)

	require.NoError(t, err)
	assert.Equal(t, []string{"monitor", "set", "reset"}, healer.calls)
	assert.Positive(t, result.RequeueAfter, "a healthy pass is repeated so a stale sentinel is healed without a CR event")
}

// One crash-looping sentinel used to fail the whole reconcile, so controller-runtime ignored
// RequeueAfter and backed off for up to ~16 minutes, leaving the other sentinels unrepaired for
// that long. Some sentinels failing is logged and retried on the normal interval; every sentinel
// failing is still an error.
func TestReconcileSentinelPerSentinelFailures(t *testing.T) {
	podErr := errors.New("dial tcp 10.0.0.2:26379: connect: connection refused")
	tests := []struct {
		name    string
		err     *commonredis.SentinelPodsError
		wantErr bool
	}{
		{name: "one of three sentinels failed: requeue on the interval", err: &commonredis.SentinelPodsError{Failed: 1, Total: 3, Errs: []error{podErr}}},
		{name: "every sentinel failed: error", err: &commonredis.SentinelPodsError{Failed: 3, Total: 3, Errs: []error{podErr, podErr, podErr}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			healer := &recordingHealer{monitorErr: tt.err}
			r, rs := newSentinelReconcilerForHealTest(t, healer)

			result, err := r.reconcileSentinel(context.Background(), rs)

			assert.Equal(t, []string{"monitor", "set", "reset"}, healer.calls)
			if tt.wantErr {
				require.ErrorIs(t, err, podErr)
				return
			}
			require.NoError(t, err)
			assert.Positive(t, result.RequeueAfter)
		})
	}
}

type masterChecker struct{ master string }

func (c *masterChecker) GetMasterFromReplication(context.Context, *rrvb2.RedisReplication) (corev1.Pod, error) {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "valkey-1"},
		Status:     corev1.PodStatus{PodIP: c.master},
	}, nil
}

func (c *masterChecker) GetPassword(context.Context, string, *commonapi.ExistingPasswordSecret) (string, error) {
	return "", nil
}

func (c *masterChecker) CheckClusterSlotsAssigned(context.Context, *rcvb2.RedisCluster) (bool, error) {
	return true, nil
}

type recordingHealer struct {
	monitorErr error
	calls      []string
}

func (h *recordingHealer) SentinelMonitor(context.Context, *rsvb2.RedisSentinel, string) error {
	h.calls = append(h.calls, "monitor")
	return h.monitorErr
}

func (h *recordingHealer) SentinelSet(context.Context, *rsvb2.RedisSentinel, string) error {
	h.calls = append(h.calls, "set")
	return nil
}

func (h *recordingHealer) SentinelReset(context.Context, *rsvb2.RedisSentinel) error {
	h.calls = append(h.calls, "reset")
	return nil
}

func (h *recordingHealer) UpdateRedisRoleLabel(context.Context, string, map[string]string, *commonapi.ExistingPasswordSecret, *commonapi.TLSConfig, string) error {
	return nil
}
