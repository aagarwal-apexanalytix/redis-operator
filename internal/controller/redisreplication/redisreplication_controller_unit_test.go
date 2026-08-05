package redisreplication

import (
	"context"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileRedisSkipsReplicationChangesWhenTopologyIsIncomplete(t *testing.T) {
	createCalled := false
	r := &Reconciler{
		K8sClient: fake.NewSimpleClientset(),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-0"}, nil
			}
			return []string{"example-replication-1"}, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return ""
		},
		CreateRedisReplicationLink: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error {
			createCalled = true
			return nil
		},
	}
	result, err := r.reconcileRedis(context.Background(), newReplicationInstanceForTest())

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, createCalled)
}

func TestReconcileRedisSkipsReplicationChangesWhenMultipleMastersAreObservedButTopologyIsIncomplete(t *testing.T) {
	createCalled := false
	r := &Reconciler{
		K8sClient: fake.NewSimpleClientset(),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-0", "example-replication-1"}, nil
			}
			return nil, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return "example-replication-1"
		},
		CreateRedisReplicationLink: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error {
			createCalled = true
			return nil
		},
	}

	result, err := r.reconcileRedis(context.Background(), newReplicationInstanceForTest())

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, createCalled)
}

func TestReconcileRedisKeepsHealthyBehaviorWhenTopologyIsComplete(t *testing.T) {
	createCalled := false
	var gotPods []string
	var gotMaster string
	r := &Reconciler{
		K8sClient: fake.NewSimpleClientset(),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-0", "example-replication-1"}, nil
			}
			return []string{"example-replication-2"}, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return "example-replication-1"
		},
		CreateRedisReplicationLink: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, pods []string, realMaster string) error {
			createCalled = true
			gotPods = append([]string{}, pods...)
			gotMaster = realMaster
			return nil
		},
	}
	result, err := r.reconcileRedis(context.Background(), newReplicationInstanceForTest())

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.True(t, createCalled)
	assert.ElementsMatch(t, []string{"example-replication-0", "example-replication-1"}, gotPods)
	assert.Equal(t, "example-replication-1", gotMaster)
}

func TestReconcileStatusStillRunsWhenOnePodIsUnobserved(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, rrvb2.AddToScheme(scheme))

	seedInstance := newReplicationInstanceForTest()
	ctrlClient := clientfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(seedInstance).
		WithObjects(seedInstance.DeepCopy()).
		Build()

	instance := &rrvb2.RedisReplication{}
	require.NoError(t, ctrlClient.Get(context.Background(), client.ObjectKeyFromObject(seedInstance), instance))

	healer := &fakeHealer{}
	r := &Reconciler{
		Client:    ctrlClient,
		K8sClient: fake.NewSimpleClientset(),
		Healer:    healer,
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-1"}, nil
			}
			return nil, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return ""
		},
	}

	result, err := r.reconcileStatus(context.Background(), instance)

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.True(t, healer.updateCalled)

	updated := &rrvb2.RedisReplication{}
	require.NoError(t, ctrlClient.Get(context.Background(), client.ObjectKeyFromObject(instance), updated))
	assert.Equal(t, "example-replication-1", updated.Status.MasterNode)
}

func TestReconcileRedisSkipsSentinelReconfigurationWhenTopologyIsIncompleteAndMasterIsAmbiguous(t *testing.T) {
	createCalled := false
	sentinelCalled := false
	r := &Reconciler{
		StatefulSet: &fakeStatefulSetService{},
		K8sClient:   fake.NewSimpleClientset(),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-0", "example-replication-1"}, nil
			}
			return nil, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return ""
		},
		CreateRedisReplicationLink: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error {
			createCalled = true
			return nil
		},
		ConfigureSentinel: func(context.Context, *rrvb2.RedisReplication, string) error {
			sentinelCalled = true
			return nil
		},
	}

	result, err := r.reconcileRedis(context.Background(), newSentinelReplicationInstanceForTest())

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, createCalled)
	assert.False(t, sentinelCalled)
}

func TestReconcileRedisConfiguresSentinelForSingleObservedMaster(t *testing.T) {
	sentinelCalled := false
	var gotMaster string
	r := &Reconciler{
		StatefulSet: &fakeStatefulSetService{},
		K8sClient:   fake.NewSimpleClientset(),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-1"}, nil
			}
			return nil, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return ""
		},
		ConfigureSentinel: func(_ context.Context, _ *rrvb2.RedisReplication, master string) error {
			sentinelCalled = true
			gotMaster = master
			return nil
		},
	}

	result, err := r.reconcileRedis(context.Background(), newSentinelReplicationInstanceForTest())

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.True(t, sentinelCalled)
	assert.Equal(t, "example-replication-1", gotMaster)
}

func newReplicationInstanceForTest() *rrvb2.RedisReplication {
	size := int32(3)
	return &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-replication",
			Namespace: "default",
		},
		Spec: rrvb2.RedisReplicationSpec{
			Size: ptr.To(size),
			KubernetesConfig: commonapi.KubernetesConfig{
				Image: "redis:7",
			},
		},
	}
}

func newSentinelReplicationInstanceForTest() *rrvb2.RedisReplication {
	instance := newReplicationInstanceForTest()
	instance.Spec.Sentinel = &rrvb2.Sentinel{Size: 3}
	return instance
}

type fakeStatefulSetService struct{}

func (f *fakeStatefulSetService) IsStatefulSetReady(context.Context, string, string) bool {
	return true
}

func (f *fakeStatefulSetService) GetStatefulSetReplicas(context.Context, string, string) int32 {
	return 0
}

type fakeHealer struct {
	updateCalled bool
}

func (f *fakeHealer) SentinelMonitor(context.Context, *rsvb2.RedisSentinel, string) error {
	return nil
}

func (f *fakeHealer) SentinelSet(context.Context, *rsvb2.RedisSentinel, string) error {
	return nil
}

func (f *fakeHealer) SentinelReset(context.Context, *rsvb2.RedisSentinel) error {
	return nil
}

func (f *fakeHealer) UpdateRedisRoleLabel(context.Context, string, map[string]string, *commonapi.ExistingPasswordSecret, *commonapi.TLSConfig) error {
	f.updateCalled = true
	return nil
}

// Regression: 2 masters + an ORPHANED replica (one that self-reports role:slave but is attached
// to nothing live, e.g. left on `replicaof <recycled-IP>` after a master reschedule) must still
// elect a master and re-stitch replication.
//
// Before the fix the fallback chain was additionally gated on len(slaveNodes)==0, so this exact
// shape — observed live on a tenant with 2 masters + 1 ghost-chasing replica — skipped the whole
// election and logged "current master could not be identified" every 30s indefinitely. The
// topology here is COMPLETE (3 observed == Spec.Size 3), so the partial-view guard must not fire.
func TestReconcileRedisElectsMasterWhenSplitBrainHasOnlyOrphanedReplicas(t *testing.T) {
	createCalled := false
	var gotPods []string
	var gotMaster string
	r := &Reconciler{
		K8sClient: fake.NewSimpleClientset(),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-0", "example-replication-1"}, nil
			}
			// self-reports slave, but no master has it attached (RealMaster below returns "")
			return []string{"example-replication-2"}, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return "" // no master has connected_slaves > 0
		},
		AttachedReplicasKnown: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) bool {
			return true // every master's replica count was readable => "" is a genuine zero
		},
		CreateRedisReplicationLink: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, pods []string, realMaster string) error {
			createCalled = true
			gotPods = append([]string{}, pods...)
			gotMaster = realMaster
			return nil
		},
	}

	result, err := r.reconcileRedis(context.Background(), newReplicationInstanceForTest())

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.True(t, createCalled, "expected replication re-stitch to run; an orphaned replica must not block master election")
	assert.ElementsMatch(t, []string{"example-replication-0", "example-replication-1"}, gotPods)
	// Status.MasterNode is empty and the fake client cannot reach redis for a best-offset probe,
	// so the last-resort bootstrap applies: masterNodes[0].
	assert.Equal(t, "example-replication-0", gotMaster)
}

// The partial-view guard must STILL hold: an incomplete topology (fewer observed pods than
// Spec.Size) never elects a master, even with orphaned replicas present.
func TestReconcileRedisStillSkipsWhenTopologyIncompleteWithOrphanedReplica(t *testing.T) {
	createCalled := false
	r := &Reconciler{
		K8sClient: fake.NewSimpleClientset(),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-0", "example-replication-1"}, nil
			}
			// An orphaned replica IS present, but only 3 of the 4 expected pods are observed.
			return []string{"example-replication-2"}, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return ""
		},
		AttachedReplicasKnown: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) bool {
			return true
		},
		CreateRedisReplicationLink: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error {
			createCalled = true
			return nil
		},
	}

	instance := newReplicationInstanceForTest()
	instance.Spec.Size = ptr.To(int32(4)) // expect 4, observe 3 => incomplete view
	result, err := r.reconcileRedis(context.Background(), instance)

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, createCalled, "incomplete topology must never elect a master")
}

// Regression (review finding 1): a STALE Status.MasterNode naming a pod that is now a SLAVE must
// never be elected. IsPodRunning alone does not catch it — the pod is running fine, it is simply
// not a master. Electing it would SLAVEOF every real master onto a slave, leaving ZERO masters;
// the next reconcile sees len(masterNodes)==0, neither branch fires, and the set is permanently
// wedged — strictly worse than the split-brain being repaired.
func TestReconcileRedisNeverElectsAStaleStatusMasterThatIsNowASlave(t *testing.T) {
	createCalled := false
	var gotMaster string
	r := &Reconciler{
		K8sClient: fake.NewSimpleClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "example-replication-2", Namespace: "default"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		}),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-0", "example-replication-1"}, nil
			}
			return []string{"example-replication-2"}, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return ""
		},
		AttachedReplicasKnown: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) bool {
			return true
		},
		CreateRedisReplicationLink: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, _ []string, realMaster string) error {
			createCalled = true
			gotMaster = realMaster
			return nil
		},
	}

	instance := newReplicationInstanceForTest()
	// Status remembers a pod that has since come back as a replica (the live incident mechanism:
	// it restarted holding `replicaof <recycled-IP>` in its emptyDir conf).
	instance.Status.MasterNode = "example-replication-2"

	result, err := r.reconcileRedis(context.Background(), instance)

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.True(t, createCalled)
	assert.NotEqual(t, "example-replication-2", gotMaster, "must never elect a pod that is not an observed master")
	assert.Contains(t, []string{"example-replication-0", "example-replication-1"}, gotMaster)
}

// Regression (review finding 2): realMaster=="" is AMBIGUOUS. checkAttachedSlave returns -1 on any
// INFO failure and GetRedisReplicationRealMaster collapses that with a genuine connected_slaves==0.
// When the replica counts could NOT be read we must abstain: a healthy master may exist, and
// electing another would SLAVEOF it into a resync that discards its writes.
func TestReconcileRedisAbstainsWhenAttachedReplicaCountsAreUnreadable(t *testing.T) {
	createCalled := false
	r := &Reconciler{
		K8sClient: fake.NewSimpleClientset(),
		RedisNodesByRole: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, role string) ([]string, error) {
			if role == "master" {
				return []string{"example-replication-0", "example-replication-1"}, nil
			}
			return []string{"example-replication-2"}, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			return "" // could mean "no replicas" OR "probe failed"
		},
		AttachedReplicasKnown: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) bool {
			return false // a probe failed => we cannot tell
		},
		CreateRedisReplicationLink: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error {
			createCalled = true
			return nil
		},
	}

	result, err := r.reconcileRedis(context.Background(), newReplicationInstanceForTest())

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.False(t, createCalled, "must not elect a master from an unreadable view (possible data loss)")
}
