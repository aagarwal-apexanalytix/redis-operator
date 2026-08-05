package redisreplication

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/service"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/statefulset"
	intctrlutil "github.com/OT-CONTAINER-KIT/redis-operator/internal/controllerutil"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/envs"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/monitoring"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	RedisReplicationFinalizer = "redisReplicationFinalizer"
	masterGroupName           = "mymaster"
)

// Reconciler reconciles a RedisReplication object
type Reconciler struct {
	client.Client
	k8sutils.StatefulSet
	Healer                     redishealer.Healer
	K8sClient                  kubernetes.Interface
	RedisReplicationTopology   func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error)
	RedisReplicationRealMaster func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string
	CreateRedisReplicationLink func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error
	AttachedReplicasKnown      func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) bool
	ConfigureSentinel          func(context.Context, *rrvb2.RedisReplication, string) error
	SentinelMonitoredMaster    func(context.Context, *rrvb2.RedisReplication, []string) (string, error)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &rrvb2.RedisReplication{}

	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		return intctrlutil.RequeueECheck(ctx, err, "failed to get RedisReplication instance")
	}

	if k8sutils.IsDeleted(instance) {
		if err := k8sutils.HandleRedisReplicationFinalizer(ctx, r.Client, instance, RedisReplicationFinalizer); err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		return intctrlutil.Reconciled()
	}

	if common.ShouldSkipReconcile(ctx, instance) {
		return intctrlutil.Reconciled()
	}

	reconcilers := []reconciler{
		{typ: "finalizer", rec: r.reconcileFinalizer},
		{typ: "resources", rec: r.reconcileResources},
		{typ: "redis", rec: r.reconcileRedis},
		{typ: "status", rec: r.reconcileStatus},
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

	return intctrlutil.RequeueAfter(ctx, time.Second*30, "")
}

// UpdateRedisReplicationMaster records masterNode as the master of the
// replication. An empty masterNode means no master was identified in this
// reconcile, which the RedisReplicationHasMaster gauge reports either way.
//
// Whether the recorded master is then cleared depends on why none was
// identified. masterAbsent says every pod was observed and none of them is a
// master: that is conclusive, and a recorded master that is provably not the
// master must not feed the bootstrap election or the sentinel controller's
// fallback, so it is cleared. Anything else - pods that could not be probed, or
// masters that could not be told apart - is not evidence that the recorded
// master changed, so the last known master is kept for those fallbacks.
func (r *Reconciler) UpdateRedisReplicationMaster(ctx context.Context, instance *rrvb2.RedisReplication, masterNode string, masterAbsent bool) error {
	if masterNode == "" {
		monitoring.RedisReplicationHasMaster.WithLabelValues(instance.Namespace, instance.Name).Set(0)
	} else {
		monitoring.RedisReplicationHasMaster.WithLabelValues(instance.Namespace, instance.Name).Set(1)
	}

	if masterNode == "" && instance.Status.MasterNode != "" {
		if masterAbsent {
			log.FromContext(ctx).Info("Every pod answered and none is a master, clearing the recorded master node",
				"statusMasterNode", instance.Status.MasterNode)
		} else {
			log.FromContext(ctx).Info("No master identified in a partial or ambiguous view, keeping the last known master node",
				"statusMasterNode", instance.Status.MasterNode)
			masterNode = instance.Status.MasterNode
		}
	}

	connectionInfo := instance.GetConnectionInfo(envs.GetServiceDNSDomain())

	if instance.Status.MasterNode == masterNode && connectionInfoEqual(instance.Status.ConnectionInfo, connectionInfo) {
		return nil
	}

	if instance.Status.MasterNode != masterNode {
		monitoring.RedisReplicationMasterRoleChangesTotal.WithLabelValues(instance.Namespace, instance.Name).Inc()
		logger := log.FromContext(ctx)
		logger.Info("Updating master node",
			"previous", instance.Status.MasterNode,
			"new", masterNode)
	}
	return r.updateStatus(ctx, instance, rrvb2.RedisReplicationStatus{
		MasterNode:     masterNode,
		ConnectionInfo: connectionInfo,
	})
}

func connectionInfoEqual(a, b *rrvb2.ConnectionInfo) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Host == b.Host && a.Port == b.Port && a.MasterName == b.MasterName
}

func (r *Reconciler) redisReplicationTopology(ctx context.Context, instance *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error) {
	if r.RedisReplicationTopology != nil {
		return r.RedisReplicationTopology(ctx, r.K8sClient, instance)
	}
	return k8sutils.GetRedisReplicationTopology(ctx, r.K8sClient, instance)
}

func (r *Reconciler) redisReplicationRealMaster(ctx context.Context, instance *rrvb2.RedisReplication, masterPods []string) string {
	if r.RedisReplicationRealMaster != nil {
		return r.RedisReplicationRealMaster(ctx, r.K8sClient, instance, masterPods)
	}
	return k8sutils.GetRedisReplicationRealMaster(ctx, r.K8sClient, instance, masterPods)
}

func (r *Reconciler) createRedisReplicationLink(ctx context.Context, instance *rrvb2.RedisReplication, pods []string, realMaster string) error {
	if r.CreateRedisReplicationLink != nil {
		return r.CreateRedisReplicationLink(ctx, r.K8sClient, instance, pods, realMaster)
	}
	return k8sutils.CreateMasterSlaveReplication(ctx, r.K8sClient, instance, pods, realMaster)
}

// attachedReplicasKnown reports whether every observed master's connected_slaves count was actually
// readable. See GetRedisReplicationAttachedReplicasKnown: a failed probe and a genuine zero both
// surface as realMaster=="" , and only the latter is safe to elect a new master from.
func (r *Reconciler) attachedReplicasKnown(ctx context.Context, instance *rrvb2.RedisReplication, masterNodes []string) bool {
	if r.AttachedReplicasKnown != nil {
		return r.AttachedReplicasKnown(ctx, r.K8sClient, instance, masterNodes)
	}
	return k8sutils.GetRedisReplicationAttachedReplicasKnown(ctx, r.K8sClient, instance, masterNodes)
}

func (r *Reconciler) configureReplicationSentinel(ctx context.Context, instance *rrvb2.RedisReplication, masterPodName string) error {
	if r.ConfigureSentinel != nil {
		return r.ConfigureSentinel(ctx, instance, masterPodName)
	}
	return r.configureSentinel(ctx, instance, masterPodName)
}

// Read PR description for context: https://github.com/OT-CONTAINER-KIT/redis-operator/pull/1843
func (r *Reconciler) observedRedisReplicationMaster(ctx context.Context, instance *rrvb2.RedisReplication, topology k8sutils.RedisReplicationTopology) (string, bool) {
	switch len(topology.Masters) {
	case 0:
		return "", false
	case 1:
		candidate := topology.Masters[0]
		if topology.Complete() {
			return candidate, true
		}
		if r.redisReplicationRealMaster(ctx, instance, topology.Masters) == candidate {
			return candidate, true
		}
		if instance.EnableSentinel() {
			monitored, err := r.sentinelMonitoredMaster(ctx, instance, topology.Masters)
			if err != nil {
				log.FromContext(ctx).Error(err, "Not trusting the only observed master: the topology is incomplete, the pod has no attached replicas and sentinel could not be asked",
					"candidate", candidate,
					"unobservedPods", topology.Unobserved)
				return "", false
			}
			if monitored == candidate {
				return candidate, true
			}
			log.FromContext(ctx).Info("Not trusting the only observed master: the topology is incomplete, the pod has no attached replicas and sentinel does not report it as master",
				"candidate", candidate,
				"statusMasterNode", instance.Status.MasterNode,
				"unobservedPods", topology.Unobserved)
			return "", false
		}
		if candidate == instance.Status.MasterNode {
			return candidate, true
		}
		log.FromContext(ctx).Info("Not trusting the only observed master: the topology is incomplete, the pod has no attached replicas and it is not the recorded master",
			"candidate", candidate,
			"statusMasterNode", instance.Status.MasterNode,
			"unobservedPods", topology.Unobserved)
		return "", false
	default:
		realMaster := r.redisReplicationRealMaster(ctx, instance, topology.Masters)
		if realMaster == "" && instance.EnableSentinel() {
			monitored, err := r.sentinelMonitoredMaster(ctx, instance, topology.Masters)
			if err != nil {
				log.FromContext(ctx).Error(err, "Could not ask sentinel which of the observed masters it monitors",
					"masters", topology.Masters)
			} else if monitored != "" {
				log.FromContext(ctx).Info("No master with attached replicas found, using the master monitored by sentinel",
					"master", monitored)
				realMaster = monitored
			}
		}
		return realMaster, realMaster != ""
	}
}

type reconciler struct {
	typ string
	rec func(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error)
}

func (r *Reconciler) reconcileFinalizer(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error) {
	if k8sutils.IsDeleted(instance) {
		if err := k8sutils.HandleRedisReplicationFinalizer(ctx, r.Client, instance, RedisReplicationFinalizer); err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		return intctrlutil.Reconciled()
	}
	if err := k8sutils.AddFinalizer(ctx, instance, RedisReplicationFinalizer, r.Client); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	return intctrlutil.Reconciled()
}

func (r *Reconciler) reconcileResources(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error) {
	if err := k8sutils.CreateReplicationRedis(ctx, instance, r.K8sClient, r.Client); err != nil {
		return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
	}
	if err := k8sutils.CreateReplicationService(ctx, instance, r.K8sClient); err != nil {
		return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
	}
	if err := k8sutils.ReconcileReplicationPodDisruptionBudget(ctx, instance, instance.Spec.PodDisruptionBudget, r.K8sClient); err != nil {
		return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
	}
	if instance.EnableSentinel() {
		svc := newSentinelService(instance)
		_, err := service.Reconcile(ctx, r.Client, svc, instance)
		if err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		sts := newSentinelStatefulSet(instance, svc.Name)
		_, err = statefulset.Reconcile(ctx, r.Client, sts, instance)
		if err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
	}
	return intctrlutil.Reconciled()
}

func (r *Reconciler) configureSentinel(ctx context.Context, inst *rrvb2.RedisReplication, masterPodName string) error {
	if masterPodName == "" {
		return nil
	}
	masterPod, err := r.K8sClient.CoreV1().Pods(inst.Namespace).Get(ctx, masterPodName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get master pod: %w", err)
	}
	var monitorAddr string
	if inst.Spec.Sentinel.ResolveHostnames == "yes" {
		monitorAddr = replicationPodHostname(inst, masterPodName)
	} else {
		monitorAddr = masterPod.Status.PodIP
	}

	if monitorAddr == "" {
		return fmt.Errorf("master pod IP not ready")
	}

	var masterPassword string
	if inst.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		secret, err := r.K8sClient.CoreV1().Secrets(inst.Namespace).Get(
			ctx,
			*inst.Spec.KubernetesConfig.ExistingPasswordSecret.Name,
			metav1.GetOptions{},
		)
		if err != nil {
			return fmt.Errorf("get master password secret: %w", err)
		}
		masterPassword = string(secret.Data[*inst.Spec.KubernetesConfig.ExistingPasswordSecret.Key])
	}

	sentinelPods, err := r.getSentinelPods(ctx, inst)
	if err != nil {
		return fmt.Errorf("get sentinel pods: %w", err)
	}

	if len(sentinelPods.Items) == 0 {
		return nil
	}

	redisClient := redis.NewClient()
	for _, pod := range sentinelPods.Items {
		if err := r.configureSentinelPod(ctx, redisClient, inst, pod, monitorAddr, masterPassword); err != nil {
			log.FromContext(ctx).Error(err, "failed to configure sentinel pod", "pod", pod.Name)
			continue
		}
	}

	return nil
}

func (r *Reconciler) getSentinelPods(ctx context.Context, inst *rrvb2.RedisReplication) (*corev1.PodList, error) {
	labels := common.GetRedisLabels(
		inst.SentinelStatefulSet(),
		common.SetupTypeSentinel,
		"sentinel",
		inst.GetLabels(),
	)

	var selector []string
	for k, v := range labels {
		selector = append(selector, fmt.Sprintf("%s=%s", k, v))
	}

	return r.K8sClient.CoreV1().Pods(inst.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: strings.Join(selector, ","),
	})
}

func (r *Reconciler) configureSentinelPod(
	ctx context.Context,
	redisClient redis.Client,
	inst *rrvb2.RedisReplication,
	sentinelPod corev1.Pod,
	masterAddr string,
	masterPassword string,
) error {
	sentinelPassword, err := r.sentinelPassword(ctx, inst)
	if err != nil {
		return err
	}

	sentinelConnInfo := &redis.ConnectionInfo{
		Host:     sentinelPod.Status.PodIP,
		Port:     "26379",
		Password: sentinelPassword,
	}

	sentinelService := redisClient.Connect(sentinelConnInfo)

	masterConnInfo := &redis.ConnectionInfo{
		Host:     masterAddr,
		Port:     "6379",
		Password: masterPassword,
	}

	quorum := int(inst.Spec.Sentinel.Size/2) + 1
	if err := sentinelService.SentinelMonitor(
		ctx,
		masterConnInfo,
		masterGroupName,
		fmt.Sprintf("%d", quorum),
	); err != nil {
		return err
	}

	for k, v := range map[string]string{
		"down-after-milliseconds": inst.Spec.Sentinel.DownAfterMilliseconds,
		"parallel-syncs":          inst.Spec.Sentinel.ParallelSyncs,
		"failover-timeout":        inst.Spec.Sentinel.FailoverTimeout,
	} {
		if v == "" {
			continue
		}
		if err := sentinelService.SentinelSet(ctx, masterGroupName, k, v); err != nil {
			return err
		}
	}

	if err := r.sentinelResetIfNeed(ctx, inst, sentinelService); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) sentinelResetIfNeed(ctx context.Context, inst *rrvb2.RedisReplication, redisService redis.Service) error {
	logger := log.FromContext(ctx)

	sentinelInfo, err := redisService.GetInfoSentinel(ctx)
	if err != nil {
		return fmt.Errorf("get sentinel info: %w", err)
	}

	var masterInfo *redis.SentinelMasterInfo
	for i := range sentinelInfo.Masters {
		if sentinelInfo.Masters[i].Name == masterGroupName {
			masterInfo = &sentinelInfo.Masters[i]
			break
		}
	}

	if masterInfo == nil {
		return fmt.Errorf("master group %s not found in sentinel info", masterGroupName)
	}

	expectedSlaves := int(*inst.Spec.Size - 1)        // Total size minus 1 master
	expectedSentinels := int(inst.Spec.Sentinel.Size) // Total sentinels minus current one

	needReset := false
	if masterInfo.Slaves != expectedSlaves {
		logger.Info("Sentinel has incorrect number of slaves, reset needed",
			"expected", expectedSlaves,
			"actual", masterInfo.Slaves)
		needReset = true
	}

	if masterInfo.Sentinels != expectedSentinels {
		logger.Info("Sentinel has incorrect number of other sentinels, reset needed",
			"expected", expectedSentinels,
			"actual", masterInfo.Sentinels)
		needReset = true
	}

	if needReset {
		if err := redisService.SentinelReset(ctx, masterGroupName); err != nil {
			return fmt.Errorf("reset sentinel: %w", err)
		}
	}

	return nil
}

func (r *Reconciler) reconcileRedis(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error) {
	if instance.EnableSentinel() {
		if !r.IsStatefulSetReady(ctx, instance.Namespace, instance.SentinelStatefulSet()) {
			return intctrlutil.RequeueAfter(ctx, time.Second*30, "waiting for sentinel statefulset to be ready")
		}
		if !r.IsStatefulSetReady(ctx, instance.Namespace, instance.RedisStatefulSet()) {
			return intctrlutil.RequeueAfter(ctx, time.Second*30, "waiting for redis statefulset to be ready")
		}
	}

	if len(instance.Spec.GetRedisDynamicConfig()) > 0 && r.IsStatefulSetReady(ctx, instance.Namespace, instance.RedisStatefulSet()) {
		if err := k8sutils.SetRedisReplicationDynamicConfig(ctx, r.K8sClient, instance); err != nil {
			return intctrlutil.RequeueE(ctx, err, "failed to set dynamic config")
		}
	}

	var realMaster string
	topology, err := r.redisReplicationTopology(ctx, instance)
	if err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	masterNodes, slaveNodes := topology.Masters, topology.Slaves
	observedPods := topology.Observed()
	incompleteTopology := !topology.Complete()
	realMaster, masterPositivelyIdentified := r.observedRedisReplicationMaster(ctx, instance, topology)
	if len(masterNodes) > 1 {
		log.FromContext(ctx).Info("Creating redis replication by executing replication creation commands")

		// Cascading fallback when no pod currently has connected_slaves > 0.
		// Only bootstrap from a COMPLETE topology so a master is never elected from a
		// partial view of the pods.
		//
		// Deliberately NOT gated on len(slaveNodes)==0. A pod that self-reports `role:slave` is
		// not evidence of a healthy replication link: after a master reschedule a replica can sit
		// on `replicaof <recycled-IP>` with master_link_status:down — a slave in name only,
		// attached to nothing live. That clause DEADLOCKED the exact split-brain this block exists
		// to repair: with 2 masters + 1 orphaned replica the whole fallback chain was skipped and
		// the reconcile logged "current master could not be identified" every 30s, forever.
		//
		// It is replaced by TWO precise guards rather than dropped outright:
		//   !incompleteTopology       — never elect from a partial view of the pods (unchanged).
		//   attachedReplicasKnown     — `realMaster == ""` is AMBIGUOUS: checkAttachedSlave returns
		//     -1 on any INFO failure and GetRedisReplicationRealMaster collapses that together with
		//     a genuine connected_slaves==0. Bootstrapping on an unreadable probe could elect a new
		//     master while a HEALTHY master/replica link exists, SLAVEOF-ing the real master into a
		//     resync that discards its writes. So we only bootstrap when every master's replica
		//     count was actually read.
		if realMaster == "" && !incompleteTopology && r.attachedReplicasKnown(ctx, instance, masterNodes) {
			// Reuse the last-known master from Status.MasterNode if it is still running, so a
			// full restart does not arbitrarily move the master.
			//
			// It MUST still be one of the observed masters. Status.MasterNode is only a memory of
			// a previous reconcile, and the pod it names can since have come back as a SLAVE —
			// precisely the incident mechanism here (a replica restarted holding
			// `replicaof <recycled-IP>` in its emptyDir conf). Electing a slave would then SLAVEOF
			// every real master onto it, leaving ZERO masters; the next reconcile sees
			// len(masterNodes)==0, neither branch fires, and the set is permanently wedged —
			// strictly worse than the split-brain being repaired. IsPodRunning alone does not
			// catch this because the pod is running perfectly well; it is just not a master.
			if instance.Status.MasterNode != "" &&
				slices.Contains(masterNodes, instance.Status.MasterNode) &&
				k8sutils.IsPodRunning(ctx, r.K8sClient, instance.Namespace, instance.Status.MasterNode) {
				log.FromContext(ctx).Info("No master with attached slaves found, falling back to Status.MasterNode",
					"statusMasterNode", instance.Status.MasterNode)
				realMaster = instance.Status.MasterNode
			}

			// Elect a new master based on redis offset. This is a best-effort attempt to pick the most up-to-date master.
			if realMaster == "" {
				bestMaster := k8sutils.GetRedisReplicationBestMaster(ctx, r.K8sClient, instance, masterNodes)
				if bestMaster != "" {
					log.FromContext(ctx).Info("No master with attached slaves found, falling back to best master based on Redis offset",
						"bestMaster", bestMaster)
					realMaster = bestMaster
				}
			}

			// Last resort: all pods are standalone masters (fresh cluster or full restart).
			// Arbitrarily pick masterNodes[0] as the new master to bootstrap replication.
			// This choice is stable within a reconcile cycle and will be corrected by
			// Status.MasterNode on subsequent cycles once replication is established.
			if realMaster == "" {
				log.FromContext(ctx).Info("No real master found via slave count or Status.MasterNode; "+
					"electing first master node as bootstrap master", "podName", masterNodes[0])
				realMaster = masterNodes[0]
			}
		}
		if incompleteTopology {
			log.FromContext(ctx).Info("Skipping replication reconfiguration because the observed topology is incomplete",
				"observedPods", observedPods,
				"unobservedPods", topology.Unobserved)
		} else if realMaster == "" {
			log.FromContext(ctx).Info("Skipping replication reconfiguration because the current master could not be identified")
		} else if err := r.createRedisReplicationLink(ctx, instance, masterNodes, realMaster); err != nil {
			return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
		}
	} else if len(masterNodes) == 1 && len(slaveNodes) > 0 {
		realMaster = masterNodes[0]

		// Continuously ENFORCE replication: re-point every replica at the single master on
		// every reconcile. SLAVEOF to the current master is idempotent (a no-op for a replica
		// already attached to it), so this is safe to run each cycle and repairs a replica that
		// has drifted off a LIVE master — e.g. one left `replicaof <recycled-IP>` (link down)
		// after a master reschedule, or `replicaof <self>` after a botched re-stitch.
		//
		// Upstream's guard (`currentRealMaster == "" && !instance.EnableSentinel()`) only
		// re-stitches when the master has ZERO attached slaves AND sentinel is disabled. In the
		// apexanalytix deployment sentinel is ENABLED and at least one replica is usually still
		// attached, so a single drifted replica falls through untouched forever: this reconcile
		// skips it, and Sentinel only reconfigures replicas during a master FAILOVER (sdown/
		// odown) — it never heals a replica that wandered off a master that is still up. Net is a
		// churn-proportional ghost-master that self-heals through neither layer.
		//
		// NOTE: upstream's RepairDisconnectedNodes (#1705) does NOT cover this — it is wired into
		// the RedisCluster controller only, not RedisReplication (our topology). (gitea iac #1135)
		//
		// The incompleteTopology guard is upstream's (#1720 class): never stitch from a partial
		// view of the pods, or a mid-rollout reconcile can point replicas at a transient master.
		// Rebase note: upstream now spells that guard with topology.Unobserved and enforces over
		// masterNodes+slaveNodes; both are adopted here. What is NOT adopted is the condition
		// upstream still wraps the whole block in — see above.
		if incompleteTopology {
			log.FromContext(ctx).Info("Skipping master-slave enforcement because the observed topology is incomplete",
				"observedPods", observedPods,
				"unobservedPods", topology.Unobserved)
		} else {
			allPods := append(masterNodes, slaveNodes...)
			if err := r.createRedisReplicationLink(ctx, instance, allPods, realMaster); err != nil {
				log.FromContext(ctx).Error(err, "Failed to enforce master-slave replication",
					"master", realMaster, "slaves", slaveNodes)
				return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
			}
			log.FromContext(ctx).Info("Successfully enforced slave replication")
		}
	}

	monitoring.RedisReplicationReplicasSizeMismatch.WithLabelValues(instance.Namespace, instance.Name).Set(0)
	if instance.Spec.Size != nil && int(*instance.Spec.Size) != observedPods {
		monitoring.RedisReplicationReplicasSizeMismatch.WithLabelValues(instance.Namespace, instance.Name).Set(1)
	}

	monitoring.RedisReplicationReplicasSizeCurrent.WithLabelValues(instance.Namespace, instance.Name).Set(float64(observedPods))
	monitoring.RedisReplicationReplicasSizeDesired.WithLabelValues(instance.Namespace, instance.Name).Set(float64(*instance.Spec.Size))

	if instance.EnableSentinel() {
		if incompleteTopology && !masterPositivelyIdentified {
			log.FromContext(ctx).Info("Skipping sentinel reconfiguration because topology is incomplete and the master is ambiguous",
				"observedPods", observedPods,
				"unobservedPods", topology.Unobserved)
		} else if err := r.configureReplicationSentinel(ctx, instance, realMaster); err != nil {
			log.FromContext(ctx).Error(err, "failed to configure sentinel")
		}
	}

	return intctrlutil.Reconciled()
}

// reconcileStatus update status and label.
func (r *Reconciler) reconcileStatus(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error) {
	topology, err := r.redisReplicationTopology(ctx, instance)
	if err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	realMaster, _ := r.observedRedisReplicationMaster(ctx, instance, topology)
	// Every pod answered and none is a master: there is no master to record, as
	// opposed to a master that could not be identified.
	masterAbsent := topology.Complete() && len(topology.Masters) == 0
	if err = r.UpdateRedisReplicationMaster(ctx, instance, realMaster, masterAbsent); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	labels := common.GetRedisLabels(instance.GetName(), common.SetupTypeReplication, "replication", instance.GetLabels())
	if err = r.Healer.UpdateRedisRoleLabel(ctx, instance.GetNamespace(), labels, instance.Spec.KubernetesConfig.ExistingPasswordSecret, instance.Spec.TLS, realMaster); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}

	if realMaster != "" {
		monitoring.RedisReplicationConnectedSlavesTotal.WithLabelValues(instance.Namespace, instance.Name).Set(float64(len(topology.Slaves)))
	} else {
		monitoring.RedisReplicationConnectedSlavesTotal.WithLabelValues(instance.Namespace, instance.Name).Set(float64(0))
	}

	return intctrlutil.Reconciled()
}

func (r *Reconciler) updateStatus(ctx context.Context, rr *rrvb2.RedisReplication, status rrvb2.RedisReplicationStatus) error {
	copy := rr.DeepCopy()
	copy.Spec = rrvb2.RedisReplicationSpec{}
	copy.Status = status
	return common.UpdateStatus(ctx, r.Client, copy)
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rrvb2.RedisReplication{}).
		WithOptions(opts).
		Complete(r)
}
