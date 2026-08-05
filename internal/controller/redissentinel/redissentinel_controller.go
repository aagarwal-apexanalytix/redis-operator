package redissentinel

import (
	"context"
	"fmt"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	intctrlutil "github.com/OT-CONTAINER-KIT/redis-operator/internal/controllerutil"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/envs"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	RedisSentinelFinalizer = "redisSentinelFinalizer"
)

// RedisSentinelReconciler reconciles a RedisSentinel object
type RedisSentinelReconciler struct {
	client.Client
	Checker            redis.Checker
	Healer             redis.Healer
	K8sClient          kubernetes.Interface
	ReplicationWatcher *intctrlutil.ResourceWatcher
}

func (r *RedisSentinelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &rsvb2.RedisSentinel{}

	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		return intctrlutil.RequeueECheck(ctx, err, "failed to get RedisSentinel instance")
	}

	if k8sutils.IsDeleted(instance) {
		if err := k8sutils.HandleRedisSentinelFinalizer(ctx, r.Client, instance, RedisSentinelFinalizer); err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		return intctrlutil.Reconciled()
	}

	if common.ShouldSkipReconcile(ctx, instance) {
		return intctrlutil.Reconciled()
	}

	reconcilers := []reconciler{
		{typ: "finalizer", rec: r.reconcileFinalizer},
		{typ: "replication", rec: r.reconcileReplication},
		{typ: "pdb", rec: r.reconcilePDB},
		{typ: "service", rec: r.reconcileService},
		{typ: "sentinel", rec: r.reconcileSentinel},
	}

	for _, reconciler := range reconcilers {
		result, err := reconciler.rec(ctx, instance)
		if err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		if result.Requeue {
			return result, nil
		}
	}

	return intctrlutil.Reconciled()
}

type reconciler struct {
	typ string
	rec func(ctx context.Context, instance *rsvb2.RedisSentinel) (ctrl.Result, error)
}

func (r *RedisSentinelReconciler) reconcileFinalizer(ctx context.Context, instance *rsvb2.RedisSentinel) (ctrl.Result, error) {
	if k8sutils.IsDeleted(instance) {
		if err := k8sutils.HandleRedisSentinelFinalizer(ctx, r.Client, instance, RedisSentinelFinalizer); err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		return intctrlutil.Reconciled()
	}
	if err := k8sutils.AddFinalizer(ctx, instance, RedisSentinelFinalizer, r.Client); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	return intctrlutil.Reconciled()
}

func (r *RedisSentinelReconciler) reconcileReplication(ctx context.Context, instance *rsvb2.RedisSentinel) (ctrl.Result, error) {
	if instance.Spec.RedisSentinelConfig != nil && !k8sutils.IsRedisReplicationReady(ctx, r.K8sClient, r.Client, instance) {
		return intctrlutil.RequeueAfter(ctx, time.Second*10, "Redis Replication is specified but not ready")
	}

	if instance.Spec.RedisSentinelConfig != nil {
		r.ReplicationWatcher.Watch(
			ctx,
			types.NamespacedName{
				Namespace: instance.Namespace,
				Name:      instance.Spec.RedisSentinelConfig.RedisReplicationName,
			},
			types.NamespacedName{
				Namespace: instance.Namespace,
				Name:      instance.Name,
			},
		)
	}
	return intctrlutil.Reconciled()
}

// sentinelMonitorAddress builds the address SENTINEL MONITOR is pointed at, returning ok=false
// when the replication master is not identifiable.
//
// That case is real and routine, not defensive padding: GetMasterFromReplication returns a
// ZERO-VALUE pod with a NIL error whenever no master has an attached replica (its loop simply
// never assigns realMasterPod) — exactly the state during a split-brain. Formatting that empty pod
// produced "..<ns>.svc.<domain>": both the pod name AND the headless-service name are empty, since
// GetHeadlessServiceNameFromPodName("") == "". SENTINEL MONITOR rejects that with
// "ERR Invalid IP address or hostname specified", and because the error aborts reconcileSentinel
// BEFORE SentinelSet and SentinelReset run, the sentinel topology is never repaired and the error
// repeats every reconcile — observed live as sentinel_masters:0 on one sentinel and num-slaves:0
// on the others, with the split-brain persisting indefinitely.
//
// Returning ok=false keeps the malformed address unconstructible rather than relying on every
// caller to remember the empty-pod case.
func sentinelMonitorAddress(master corev1.Pod, namespace, dnsDomain string, resolveHostnames bool) (string, bool) {
	if master.Name == "" {
		return "", false
	}
	if resolveHostnames {
		return fmt.Sprintf("%s.%s.%s.svc.%s",
			master.Name, common.GetHeadlessServiceNameFromPodName(master.Name), namespace, dnsDomain), true
	}
	// Same class of hole on the IP path: a pod that exists but has no IP assigned yet.
	if master.Status.PodIP == "" {
		return "", false
	}
	return master.Status.PodIP, true
}

func (r *RedisSentinelReconciler) reconcileSentinel(ctx context.Context, instance *rsvb2.RedisSentinel) (ctrl.Result, error) {
	if err := k8sutils.CreateRedisSentinel(ctx, r.K8sClient, instance, r.K8sClient, r.Client); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	if instance.Spec.RedisSentinelConfig == nil {
		return intctrlutil.Reconciled()
	}

	rr := &rrvb2.RedisReplication{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: instance.Namespace,
		Name:      instance.Spec.RedisSentinelConfig.RedisReplicationName,
	}, rr); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}

	var monitorAddr string
	if master, err := r.Checker.GetMasterFromReplication(ctx, rr); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	} else {
		var ok bool
		monitorAddr, ok = sentinelMonitorAddress(master, rr.Namespace, envs.GetServiceDNSDomain(),
			instance.Spec.RedisSentinelConfig.ResolveHostnames == "yes")
		if !ok {
			// Electing the master is the RedisReplication controller's job, not ours. Requeue quietly
			// and let it converge; sending a knowingly-invalid address helps nobody and hides the state.
			log.FromContext(ctx).Info("Skipping sentinel monitor: replication has no identifiable master yet",
				"redisReplication", rr.Name)
			return intctrlutil.RequeueAfter(ctx, time.Second*30, "")
		}
	}
	if err := r.Healer.SentinelMonitor(ctx, instance, monitorAddr); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	if err := r.Healer.SentinelSet(ctx, instance, monitorAddr); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	if err := r.Healer.SentinelReset(ctx, instance); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	return intctrlutil.Reconciled()
}

func (r *RedisSentinelReconciler) reconcilePDB(ctx context.Context, instance *rsvb2.RedisSentinel) (ctrl.Result, error) {
	if err := k8sutils.ReconcileSentinelPodDisruptionBudget(ctx, instance, instance.Spec.PodDisruptionBudget, r.K8sClient); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	return intctrlutil.Reconciled()
}

func (r *RedisSentinelReconciler) reconcileService(ctx context.Context, instance *rsvb2.RedisSentinel) (ctrl.Result, error) {
	if err := k8sutils.CreateRedisSentinelService(ctx, instance, r.K8sClient); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	return intctrlutil.Reconciled()
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisSentinelReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rsvb2.RedisSentinel{}).
		Owns(&appsv1.StatefulSet{}).
		WithOptions(opts).
		Watches(&rrvb2.RedisReplication{}, r.ReplicationWatcher).
		Complete(r)
}
