package k8sutils

import (
	"testing"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/features"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestSentinelContainerMountsConfigVolume(t *testing.T) {
	// Ensure GenerateConfigInInitContainer is disabled (default)
	if err := features.MutableFeatureGate.Set("GenerateConfigInInitContainer=false"); err != nil {
		t.Fatalf("failed to set feature gate: %v", err)
	}

	containers := generateContainerDef(
		"redis-sentinel-sentinel",
		containerParameters{
			Role:            "sentinel",
			Image:           "quay.io/opstree/redis-sentinel:v8.2.2",
			ImagePullPolicy: corev1.PullIfNotPresent,
		},
		false, // clusterMode
		false, // nodeConfVolume
		false, // enableMetrics
		nil,   // externalConfig
		nil,   // clusterVersion
		nil,   // mountpath
		nil,   // sidecars
	)

	assert.Len(t, containers, 1, "should have exactly one container")

	// Verify the config volume is mounted at /etc/redis
	var found bool
	for _, vm := range containers[0].VolumeMounts {
		if vm.Name == common.VolumeNameConfig && vm.MountPath == "/etc/redis" {
			found = true
			break
		}
	}
	assert.True(t, found, "sentinel container should mount the config volume at /etc/redis for sentinel.conf persistence")
}

func TestNonSentinelContainerDoesNotMountConfigVolumeByDefault(t *testing.T) {
	// Ensure GenerateConfigInInitContainer is disabled (default)
	if err := features.MutableFeatureGate.Set("GenerateConfigInInitContainer=false"); err != nil {
		t.Fatalf("failed to set feature gate: %v", err)
	}

	containers := generateContainerDef(
		"redis",
		containerParameters{
			Role:            "master",
			Image:           "redis:latest",
			ImagePullPolicy: corev1.PullAlways,
		},
		false, false, false, nil, nil, nil, nil,
	)

	assert.Len(t, containers, 1)

	for _, vm := range containers[0].VolumeMounts {
		if vm.Name == common.VolumeNameConfig && vm.MountPath == "/etc/redis" {
			t.Error("non-sentinel container should NOT mount config volume when GenerateConfigInInitContainer is disabled")
		}
	}
}

// TestSentinelWithGenerateConfigInInitContainer pins the three properties that make
// GenerateConfigInInitContainer a SAFE fix for the duplicate-`sentinel monitor` crashloop.
//
// Background. The sentinel image entrypoint (OT-CONTAINER-KIT/redis, entrypoint-sentinel.sh) does:
//
//	{ ... echo "sentinel monitor ${MASTER_GROUP_NAME} ${IP} ${PORT} ${QUORUM}" ... } >> /etc/redis/sentinel.conf
//	exec redis-sentinel /etc/redis/sentinel.conf
//
// It APPENDS on every container start and never truncates. Sentinel then rewrites the conf at runtime
// with the resolved master, so ANY container restart appends a SECOND monitor line under the same
// master name and redis fatals on config parse ("Duplicate master name."), crashlooping forever —
// the config volume is an emptyDir, which survives container restarts, so it can never self-heal.
// Observed twice on the same pod in production (2026-07-28: 171 restarts; 2026-08-23: 719 restarts).
//
// Enabling this gate fixes it at both ends:
//  1. the init container regenerates the conf with os.WriteFile (truncate + overwrite), so duplicates
//     cannot accumulate. Its config mount is unconditional in generateInitContainerDef's enabled
//     branch, so it holds by construction and is not re-asserted here;
//  2. the main container's command is overridden to exec redis-sentinel directly, so the APPENDING
//     shell entrypoint never runs at all;
//  3. the main container must still MOUNT the config volume, or it would read /etc/redis/sentinel.conf
//     from its own image layer and never see what the init container generated.
//
// This test asserts (2) and (3). (3) is the one that genuinely needs a regression test: the fork adds
// an unconditional sentinel mount only on the gate-DISABLED path, so the ENABLED path depends entirely
// on getVolumeMount's own feature-gated append. If that ever moves, the gate silently produces a
// sentinel reading the wrong file — a failure that looks like "the fix did nothing".
func TestSentinelWithGenerateConfigInInitContainer(t *testing.T) {
	if err := features.MutableFeatureGate.Set("GenerateConfigInInitContainer=true"); err != nil {
		t.Fatalf("failed to set feature gate: %v", err)
	}
	t.Cleanup(func() {
		_ = features.MutableFeatureGate.Set("GenerateConfigInInitContainer=false")
	})

	containers := generateContainerDef(
		"redis-sentinel-sentinel",
		containerParameters{
			Role:            "sentinel",
			Image:           "quay.io/opstree/redis-sentinel:v8.6.2",
			ImagePullPolicy: corev1.PullIfNotPresent,
		},
		false, false, false, nil, nil, nil, nil,
	)
	assert.Len(t, containers, 1, "should have exactly one container")

	// (3) the main container must see the init container's generated conf
	var mounts int
	for _, vm := range containers[0].VolumeMounts {
		if vm.Name == common.VolumeNameConfig && vm.MountPath == "/etc/redis" {
			mounts++
		}
	}
	assert.Equal(t, 1, mounts,
		"with GenerateConfigInInitContainer enabled the sentinel container must mount the config volume at /etc/redis EXACTLY once "+
			"(zero => it reads sentinel.conf from its image layer and ignores the generated one; twice => duplicate mount, invalid pod spec)")

	// (2) the appending shell entrypoint must be bypassed
	assert.Equal(t, []string{"redis-sentinel"}, containers[0].Command,
		"the appending entrypoint-sentinel.sh must be bypassed by exec'ing redis-sentinel directly")
	assert.Equal(t, []string{"/etc/redis/sentinel.conf"}, containers[0].Args,
		"redis-sentinel must be pointed at the conf the init container generated")
}
