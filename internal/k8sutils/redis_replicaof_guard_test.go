package k8sutils

import (
	"context"
	"testing"

	"github.com/go-redis/redismock/v9"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// Captured from a real valkey 8 replica in this fleet (INFO Replication), trimmed to the fields
// under test plus enough surrounding lines that a prefix-matching bug would show up.
const replicaInfoAttached = "# Replication\r\n" +
	"role:slave\r\n" +
	"master_host:valkey-0.valkey-headless.apexportal-is.svc.cluster.local\r\n" +
	"master_port:6379\r\n" +
	"master_link_status:up\r\n" +
	"master_last_io_seconds_ago:1\r\n" +
	"slave_read_only:1\r\n" +
	"master_repl_offset:118231\r\n"

const masterInfo = "# Replication\r\n" +
	"role:master\r\n" +
	"connected_slaves:2\r\n" +
	"master_failover_state:no-failover\r\n" +
	"master_repl_offset:118231\r\n"

func Test_parseReplicationTarget(t *testing.T) {
	tests := []struct {
		name      string
		info      string
		wantHost  string
		wantPort  string
		wantKnown bool
	}{
		{
			name:      "replica reports its master",
			info:      replicaInfoAttached,
			wantHost:  "valkey-0.valkey-headless.apexportal-is.svc.cluster.local",
			wantPort:  "6379",
			wantKnown: true,
		},
		{
			// A master emits neither field, so the target is genuinely UNKNOWN — not "empty".
			name:      "master has no replication target",
			info:      masterInfo,
			wantKnown: false,
		},
		{
			// A replica stranded on a recycled Pod IP: still a perfectly readable target, and one
			// that MUST NOT match the FQDN we want, so the re-stitch still fires.
			name: "replica pinned to a raw pod IP",
			info: "# Replication\r\nrole:slave\r\n" +
				"master_host:10.244.3.17\r\nmaster_port:6379\r\n" +
				"master_link_status:down\r\n",
			wantHost:  "10.244.3.17",
			wantPort:  "6379",
			wantKnown: true,
		},
		{
			name:      "truncated info carrying only the host",
			info:      "# Replication\r\nrole:slave\r\nmaster_host:valkey-0.valkey-headless.ns.svc.cluster.local\r\n",
			wantHost:  "valkey-0.valkey-headless.ns.svc.cluster.local",
			wantKnown: false,
		},
		{
			name:      "empty info",
			info:      "",
			wantKnown: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, known := parseReplicationTarget(tt.info)
			assert.Equal(t, tt.wantKnown, known)
			if tt.wantKnown {
				assert.Equal(t, tt.wantHost, host)
				assert.Equal(t, tt.wantPort, port)
			}
		})
	}
}

func Test_replicationTargetMatches(t *testing.T) {
	const wantAddr = "valkey-0.valkey-headless.apexportal-is.svc.cluster.local"

	tests := []struct {
		name  string
		info  string
		addr  string
		port  string
		match bool
	}{
		{
			name:  "already attached to the elected master",
			info:  replicaInfoAttached,
			addr:  wantAddr,
			port:  "6379",
			match: true,
		},
		{
			// DNS names are case-insensitive; the operator always builds lowercase, but a replica
			// stitched by another actor need not have been given that exact spelling.
			name:  "case-insensitive hostname match",
			info:  replicaInfoAttached,
			addr:  "VALKEY-0.Valkey-Headless.apexportal-is.SVC.cluster.local",
			port:  "6379",
			match: true,
		},
		{
			name:  "attached to a different master",
			info:  replicaInfoAttached,
			addr:  "valkey-1.valkey-headless.apexportal-is.svc.cluster.local",
			port:  "6379",
			match: false,
		},
		{
			// Cross-namespace pollution — the exact tenant-isolation class the FQDN pin exists for.
			name: "attached to a FOREIGN namespace's master",
			info: "# Replication\r\nrole:slave\r\n" +
				"master_host:valkey-0.valkey-headless.other-tenant.svc.cluster.local\r\n" +
				"master_port:6379\r\nmaster_link_status:up\r\n",
			addr:  wantAddr,
			port:  "6379",
			match: false,
		},
		{
			name:  "right host, wrong port",
			info:  replicaInfoAttached,
			addr:  wantAddr,
			port:  "6380",
			match: false,
		},
		{
			name:  "a master never matches",
			info:  masterInfo,
			addr:  wantAddr,
			port:  "6379",
			match: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.match, replicationTargetMatches(tt.info, tt.addr, tt.port))
		})
	}
}

func Test_isAlreadyReplicaOf(t *testing.T) {
	const wantAddr = "valkey-0.valkey-headless.apexportal-is.svc.cluster.local"

	t.Run("already attached", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		mock.ExpectInfo("Replication").SetVal(replicaInfoAttached)

		assert.True(t, isAlreadyReplicaOf(context.TODO(), client, "valkey-1", wantAddr, redisReplicationPort))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("drifted onto a recycled pod IP", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		mock.ExpectInfo("Replication").SetVal(
			"# Replication\r\nrole:slave\r\nmaster_host:10.244.3.17\r\nmaster_port:6379\r\n" +
				"master_link_status:down\r\n")

		assert.False(t, isAlreadyReplicaOf(context.TODO(), client, "valkey-1", wantAddr, redisReplicationPort))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("fails open when INFO cannot be read", func(t *testing.T) {
		// The whole point of the guard is suppressing a no-op command; an unreadable probe must
		// degrade to the pre-guard behaviour (issue SLAVEOF), never to silently skipping a repair.
		client, mock := redismock.NewClientMock()
		mock.ExpectInfo("Replication").SetErr(redis.ErrClosed)

		assert.False(t, isAlreadyReplicaOf(context.TODO(), client, "valkey-1", wantAddr, redisReplicationPort))
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
