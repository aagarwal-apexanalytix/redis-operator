package redissentinel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Regression for the split-brain sentinel stall.
//
// GetMasterFromReplication returns a ZERO-VALUE pod with a NIL error when no master has an attached
// replica. The old code formatted that straight into the monitor address, producing
// "..<ns>.svc.<domain>" — SENTINEL MONITOR answers "ERR Invalid IP address or hostname specified",
// which aborted reconcileSentinel before SentinelSet/SentinelReset ran, so sentinel was never
// repaired and the error repeated forever.
func TestSentinelMonitorAddress(t *testing.T) {
	tests := []struct {
		name             string
		master           corev1.Pod
		resolveHostnames bool
		wantAddr         string
		wantOK           bool
	}{
		{
			name:             "empty master (split-brain: no master has an attached replica) is rejected",
			master:           corev1.Pod{},
			resolveHostnames: true,
			wantAddr:         "",
			wantOK:           false,
		},
		{
			name:             "empty master is rejected on the IP path too",
			master:           corev1.Pod{},
			resolveHostnames: false,
			wantAddr:         "",
			wantOK:           false,
		},
		{
			name:             "real master resolves to the headless FQDN",
			master:           corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "valkey-1"}},
			resolveHostnames: true,
			wantAddr:         "valkey-1.valkey-headless.uat6-maruti.svc.cluster.local",
			wantOK:           true,
		},
		{
			name: "real master resolves to its pod IP when hostnames are disabled",
			master: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "valkey-1"},
				Status:     corev1.PodStatus{PodIP: "10.116.2.13"},
			},
			resolveHostnames: false,
			wantAddr:         "10.116.2.13",
			wantOK:           true,
		},
		{
			name:             "named pod with no IP yet is rejected rather than sending an empty address",
			master:           corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "valkey-1"}},
			resolveHostnames: false,
			wantAddr:         "",
			wantOK:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, ok := sentinelMonitorAddress(tt.master, "uat6-maruti", "cluster.local", tt.resolveHostnames)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantAddr, addr)
			// Runs on EVERY case, not just failures: the bug produced "..<ns>.svc.<domain>",
			// so this must be able to fail on a success path too.
			assert.NotContains(t, addr, "..", "a leading-dot address must never be produced")
		})
	}
}
