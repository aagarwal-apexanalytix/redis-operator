package k8sutils

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func sentinelContainer(t *testing.T, params containerParameters) corev1.Container {
	t.Helper()
	containers := generateContainerDef("valkey-sentinel", params, false, false, false, nil, nil, nil, nil)
	require.NotEmpty(t, containers)
	return containers[0]
}

func probeScript(t *testing.T, p *corev1.Probe) string {
	t.Helper()
	require.NotNil(t, p)
	require.NotNil(t, p.Exec)
	require.Len(t, p.Exec.Command, 3)
	assert.Equal(t, []string{"sh", "-ec"}, p.Exec.Command[:2])
	return p.Exec.Command[2]
}

func TestSentinelReadinessProbe_Generation(t *testing.T) {
	pingOnly := func(sentinel, tls bool) string { return pingCheckScript(sentinel, tls) }

	tests := []struct {
		name            string
		params          containerParameters
		wantMasterCheck bool
		tls             bool
	}{
		{
			name:            "operator-managed sentinel: readiness waits for a real master",
			params:          containerParameters{Role: "sentinel", SentinelReadinessRequiresMaster: true},
			wantMasterCheck: true,
		},
		{
			name:            "operator-managed sentinel with TLS: the master check uses TLS too",
			params:          containerParameters{Role: "sentinel", SentinelReadinessRequiresMaster: true, TLSConfig: &commonapi.TLSConfig{}},
			wantMasterCheck: true,
			tls:             true,
		},
		{
			name:   "sentinel the operator does not manage: PING only, as before",
			params: containerParameters{Role: "sentinel"},
		},
		{
			name:   "non-sentinel role ignores the flag",
			params: containerParameters{Role: "replication", SentinelReadinessRequiresMaster: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := sentinelContainer(t, tt.params)
			sentinel := tt.params.Role == "sentinel"

			readiness := probeScript(t, c.ReadinessProbe)
			liveness := probeScript(t, c.LivenessProbe)

			// Liveness is PING-only for every role: a sentinel waiting for the operator to set
			// its master must not be restarted.
			assert.Equal(t, pingOnly(sentinel, tt.tls), liveness)
			assert.NotContains(t, liveness, "get-master-addr-by-name")

			if !tt.wantMasterCheck {
				assert.Equal(t, pingOnly(sentinel, tt.tls), readiness, "readiness must be byte-identical to the PING probe so these pods do not roll")
				return
			}
			assert.True(t, strings.HasPrefix(readiness, pingOnly(true, tt.tls)+"\n"), "the master check extends the PING probe")
			assert.Contains(t, readiness, `sentinel get-master-addr-by-name "${MASTER_GROUP_NAME:-mymaster}"`)
			assert.Contains(t, readiness, `[ "$1" != "0.0.0.0" ]`)
			if tt.tls {
				assert.Equal(t, 2, strings.Count(readiness, "--tls"), "both redis-cli calls must use TLS")
			}
		})
	}
}

func TestSentinelReadinessProbe_KeepsCRSettings(t *testing.T) {
	t.Run("timings from the CR are kept", func(t *testing.T) {
		c := sentinelContainer(t, containerParameters{
			Role:                            "sentinel",
			SentinelReadinessRequiresMaster: true,
			ReadinessProbe:                  &corev1.Probe{TimeoutSeconds: 5, PeriodSeconds: 10, FailureThreshold: 3, InitialDelaySeconds: 10},
		})
		assert.Equal(t, int32(5), c.ReadinessProbe.TimeoutSeconds)
		assert.Equal(t, int32(10), c.ReadinessProbe.PeriodSeconds)
		assert.Equal(t, int32(3), c.ReadinessProbe.FailureThreshold)
		assert.Contains(t, probeScript(t, c.ReadinessProbe), "get-master-addr-by-name")
	})

	t.Run("a handler supplied in the CR is never replaced", func(t *testing.T) {
		custom := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}
		c := sentinelContainer(t, containerParameters{
			Role:                            "sentinel",
			SentinelReadinessRequiresMaster: true,
			ReadinessProbe:                  custom,
		})
		assert.Equal(t, []string{"true"}, c.ReadinessProbe.Exec.Command)
	})
}

func Test_generateRedisSentinelContainerParams_ReadinessRequiresMaster(t *testing.T) {
	withConfig := &rsvb2.RedisSentinel{}
	withConfig.Spec.RedisSentinelConfig = &rsvb2.RedisSentinelConfig{}
	withConfig.Spec.RedisSentinelConfig.RedisReplicationName = "valkey"
	withConfig.Spec.RedisSentinelConfig.MasterGroupName = "mymaster"

	got, err := generateRedisSentinelContainerParams(context.TODO(), nil, withConfig, nil, nil, nil)
	require.NoError(t, err)
	assert.True(t, got.SentinelReadinessRequiresMaster, "the operator sets this sentinel's master, so readiness may wait for it")

	got, err = generateRedisSentinelContainerParams(context.TODO(), nil, &rsvb2.RedisSentinel{}, nil, nil, nil)
	require.NoError(t, err)
	assert.False(t, got.SentinelReadinessRequiresMaster, "nothing would ever replace a placeholder master here")
}

// TestSentinelReadinessScript_Decision EXECUTES the generated readiness and liveness scripts
// with a stub redis-cli, so the shell logic itself is what is tested, not its text.
func TestSentinelReadinessScript_Decision(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh not available")
	}

	readiness := getSentinelReadinessProbe(nil, false).Exec.Command[2]
	liveness := getProbeInfo(nil, true, false).Exec.Command[2]

	stubDir := t.TempDir()
	stub := `#!/bin/sh
printf '%s\n' "$*" >> "$STUB_LOG"
case "$*" in
  *" ping") [ -n "$STUB_PING" ] && printf '%s\n' "$STUB_PING"; exit "${STUB_RC:-0}" ;;
  *get-master-addr-by-name*) printf "$STUB_MASTER"; exit "${STUB_RC:-0}" ;;
esac
exit 99
`
	require.NoError(t, os.WriteFile(filepath.Join(stubDir, "redis-cli"), []byte(stub), 0o755))

	tests := []struct {
		name          string
		ping          string
		master        string // printf format, as redis-cli prints the reply
		rc            string
		groupEnv      *string
		wantReady     bool
		wantLive      bool
		wantGroupArgs string
	}{
		{name: "real master IP", ping: "PONG", master: `10.0.1.10\n6379\n`, wantReady: true, wantLive: true},
		{name: "master announced as a hostname", ping: "PONG", master: `valkey-0.valkey-headless.ns.svc.cluster.local\n6379\n`, wantReady: true, wantLive: true},
		{name: "bootstrap placeholder 0.0.0.0", ping: "PONG", master: `0.0.0.0\n6379\n`, wantReady: false, wantLive: true},
		{name: "group not monitored (nil reply)", ping: "PONG", master: ``, wantReady: false, wantLive: true},
		{name: "error reply printed with exit 0", ping: "PONG", master: `ERR unknown subcommand 'get-master-addr-by-name'\n`, wantReady: false, wantLive: true},
		{name: "NOAUTH printed with exit 0", ping: "PONG", master: `NOAUTH Authentication required.\n`, wantReady: false, wantLive: true},
		{name: "two-word error reply", ping: "PONG", master: `WRONGPASS denied\n`, wantReady: false, wantLive: true},
		{name: "sentinel unreachable", ping: "", master: ``, rc: "1", wantReady: false, wantLive: false},
		{name: "PING answers something other than PONG", ping: "LOADING", master: `10.0.1.10\n6379\n`, wantReady: false, wantLive: false},
		{name: "group name defaults like the bootstrap", ping: "PONG", master: `10.0.1.10\n6379\n`, groupEnv: new(string), wantReady: true, wantLive: true, wantGroupArgs: "get-master-addr-by-name mymaster"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logPath := filepath.Join(t.TempDir(), "calls.log")
			group := "master"
			if tt.groupEnv != nil {
				group = *tt.groupEnv
			}
			env := []string{
				"PATH=" + stubDir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"SENTINEL_PORT=26379",
				"MASTER_GROUP_NAME=" + group,
				"STUB_LOG=" + logPath,
				"STUB_PING=" + tt.ping,
				"STUB_MASTER=" + tt.master,
				"STUB_RC=" + tt.rc,
			}
			run := func(script string) bool {
				cmd := exec.CommandContext(context.Background(), sh, "-ec", script)
				cmd.Env = env
				return cmd.Run() == nil
			}

			assert.Equal(t, tt.wantReady, run(readiness), "readiness")
			assert.Equal(t, tt.wantLive, run(liveness), "liveness")

			calls, err := os.ReadFile(logPath)
			require.NoError(t, err)
			want := tt.wantGroupArgs
			if want == "" {
				want = "get-master-addr-by-name master"
			}
			if tt.wantReady {
				assert.Contains(t, string(calls), want)
				assert.Contains(t, string(calls), "-p 26379")
			}
		})
	}
}
