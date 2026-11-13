package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	rediscli "github.com/redis/go-redis/v9"
)

// ErrMonitorTargetUnresolved means a hostname SENTINEL MONITOR target did not resolve, so
// whether a sentinel reporting an IP is stale could not be decided and it was left alone.
var ErrMonitorTargetUnresolved = errors.New("sentinel monitor target does not resolve")

const (
	redisRoleMaster = "role:master"

	// resolveTimeout bounds one DNS lookup of a SENTINEL MONITOR target.
	resolveTimeout = 2 * time.Second
)

type ConnectionInfo struct {
	// Host is the IP address or hostname for connection
	Host string
	// Port is the port for redis or sentinel
	Port     string
	Password string
	// TLSConfig configuration, nil means TLS is disabled
	TLSConfig *tls.Config
	// Resolve, when set, returns the current IPs of a hostname Host. A SENTINEL MONITOR target
	// calls it only when a sentinel reports the master as an IP, so a caller can resolve once per
	// pass and not at all when every sentinel reports the hostname. When nil, the hostname is
	// resolved per call.
	Resolve func(ctx context.Context) ([]string, error)
}

// ClusterStatus cluster status information, including the number of assigned slots
type ClusterStatus struct {
	SlotsAssigned int // Total number of assigned slots
}

// GetAddress returns the connection address
func (c *ConnectionInfo) GetAddress() string {
	return net.JoinHostPort(c.Host, c.Port)
}

type Client interface {
	Connect(info *ConnectionInfo) Service
}

func (c *client) Connect(info *ConnectionInfo) Service {
	service := &service{}
	service.connectionInfo = info
	return service
}

type client struct{}

func NewClient() Client {
	return &client{}
}

type Service interface {
	IsMaster(ctx context.Context) (bool, error)
	GetAttachedReplicaCount(ctx context.Context) (int, error)
	SentinelMonitor(ctx context.Context, master *ConnectionInfo, masterGroupName, quorum string) error
	SentinelSet(ctx context.Context, masterGroupName, key, value string) error
	SentinelReset(ctx context.Context, masterGroupName string) error
	GetInfoSentinel(ctx context.Context) (*InfoSentinelResult, error)
	// SentinelPeers returns the replicas and other sentinels a sentinel knows for a master group.
	SentinelPeers(ctx context.Context, masterGroupName string) (*SentinelPeersResult, error)
	GetClusterInfo(ctx context.Context) (*ClusterStatus, error)
}

type InfoSentinelResult struct {
	Masters []SentinelMasterInfo
}

type SentinelMasterInfo struct {
	Name      string
	Status    string
	Address   string
	Slaves    int
	Sentinels int
}

// SentinelPeer is one replica or sentinel entry of SENTINEL REPLICAS / SENTINEL SENTINELS.
type SentinelPeer struct {
	IP    string
	Port  string
	Flags string
}

type SentinelPeersResult struct {
	Replicas  []SentinelPeer
	Sentinels []SentinelPeer
}

type service struct {
	connectionInfo *ConnectionInfo
}

func NewService() Service {
	return &service{}
}

func (s *service) createClient() *rediscli.Client {
	if s.connectionInfo == nil {
		return nil
	}
	opts := &rediscli.Options{
		Addr:     s.connectionInfo.GetAddress(),
		Password: s.connectionInfo.Password,
		DB:       0,
	}
	if s.connectionInfo.TLSConfig != nil {
		opts.TLSConfig = s.connectionInfo.TLSConfig
	}
	return rediscli.NewClient(opts)
}

func (c *service) GetInfoSentinel(ctx context.Context) (*InfoSentinelResult, error) {
	client := c.createClient()
	if client == nil {
		return nil, nil
	}
	defer client.Close()

	result, err := client.Info(ctx, "sentinel").Result()
	if err != nil {
		return nil, err
	}

	info := &InfoSentinelResult{}

	// Parse sentinel section
	// Expected format: master0:name=myMaster,status=ok,address=10.233.93.209:6379,slaves=2,sentinels=1
	for _, line := range strings.Split(result, "\r\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "master") && strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}

			masterData := make(map[string]string)
			for _, pair := range strings.Split(parts[1], ",") {
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) == 2 {
					masterData[kv[0]] = kv[1]
				}
			}

			master := SentinelMasterInfo{
				Name:    masterData["name"],
				Status:  masterData["status"],
				Address: masterData["address"],
			}
			if slavesStr, ok := masterData["slaves"]; ok {
				if slaves, err := strconv.Atoi(slavesStr); err == nil {
					master.Slaves = slaves
				}
			}
			if sentinelsStr, ok := masterData["sentinels"]; ok {
				if sentinels, err := strconv.Atoi(sentinelsStr); err == nil {
					master.Sentinels = sentinels
				}
			}

			info.Masters = append(info.Masters, master)
		}
	}

	return info, nil
}

func (c *service) SentinelSet(ctx context.Context, masterGroupName, key, value string) error {
	client := c.createClient()
	if client == nil {
		return nil
	}
	defer client.Close()

	// Every SENTINEL SET rewrites the sentinel's config file, and the reconcile repeats on a
	// timer, so a value the sentinel already holds is not sent again.
	if current, err := client.Do(ctx, "SENTINEL", "MASTER", masterGroupName).Result(); err == nil {
		if have, ok := sentinelFields(current)[key]; ok && have == value {
			return nil
		}
	}

	cmd := rediscli.NewStringCmd(ctx, "SENTINEL", "SET", masterGroupName, key, value)
	err := client.Process(ctx, cmd)
	if err != nil {
		return err
	}
	if err = cmd.Err(); err != nil {
		return err
	}
	return nil
}

func (c *service) SentinelReset(ctx context.Context, masterGroupName string) error {
	client := c.createClient()
	if client == nil {
		return nil
	}
	defer client.Close()

	cmd := rediscli.NewStringCmd(ctx, "SENTINEL", "RESET", masterGroupName)
	err := client.Process(ctx, cmd)
	if err != nil {
		return err
	}
	if err = cmd.Err(); err != nil {
		return err
	}
	return nil
}

func (c *service) SentinelMonitor(ctx context.Context, master *ConnectionInfo, masterGroupName, quorum string) error {
	var (
		cmd *rediscli.BoolCmd
		err error
	)

	client := c.createClient()
	if client == nil {
		return nil
	}
	defer client.Close()

	masterExists := false
	if current, err := client.Do(ctx, "SENTINEL", "MASTER", masterGroupName).Result(); err == nil {
		masterExists = true
		fields := sentinelFields(current)
		same, err := monitorsTarget(ctx, fields["ip"], fields["port"], master)
		if err != nil {
			// The target cannot be resolved, so whether this sentinel is stale is unknown.
			// SENTINEL MONITOR would fail on the same name, and the REMOVE before it would
			// leave the sentinel monitoring nothing, so leave it as it is.
			return err
		}
		if same {
			return nil
		}
	}

	// Only remove if master was already being monitored (with wrong host/port).
	// On first-time setup, SENTINEL REMOVE would fail with "ERR No such master
	// with that name" and prevent SENTINEL MONITOR from ever being called.
	if masterExists {
		cmd = rediscli.NewBoolCmd(ctx, "SENTINEL", "REMOVE", masterGroupName)
		err = client.Process(ctx, cmd)
		if err != nil {
			return err
		}
		if err = cmd.Err(); err != nil {
			return err
		}
	}

	cmd = rediscli.NewBoolCmd(ctx, "SENTINEL", "MONITOR", masterGroupName, master.Host, master.Port, quorum)
	err = client.Process(ctx, cmd)
	if err != nil {
		return err
	}
	if err = cmd.Err(); err != nil {
		return err
	}

	if master.Password != "" {
		cmd = rediscli.NewBoolCmd(ctx, "SENTINEL", "SET", masterGroupName, "auth-pass", master.Password)
		err = client.Process(ctx, cmd)
		if err != nil {
			return err
		}
		if err = cmd.Err(); err != nil {
			return err
		}
	}

	return nil
}

// monitorsTarget reports whether a sentinel that reports its master at monitoredHost:monitoredPort
// already monitors target.
//
// A hostname target has to be resolved first. With resolve-hostnames yes and announce-hostnames
// no, Sentinel resolves the name once at MONITOR time and reports only the resulting IP, so a
// plain string comparison never matches: every reconcile then removed and re-added the master,
// discarding the sentinel's known replicas and sentinels and resetting its config epoch. Comparing
// against the name's current addresses leaves a sentinel on the current master alone and still
// re-points one left on the IP of a master pod that has since moved.
func monitorsTarget(ctx context.Context, monitoredHost, monitoredPort string, target *ConnectionInfo) (bool, error) {
	if monitoredPort != target.Port {
		return false, nil
	}
	if monitoredHost == target.Host {
		return true, nil
	}
	if net.ParseIP(target.Host) != nil {
		return false, nil
	}
	monitoredIP := net.ParseIP(monitoredHost)
	if monitoredIP == nil {
		// Sentinel reports a different hostname (announce-hostnames yes): a different master.
		return false, nil
	}
	resolve := target.Resolve
	if resolve == nil {
		resolve = func(ctx context.Context) ([]string, error) { return ResolveHost(ctx, target.Host) }
	}
	addrs, err := resolve(ctx)
	if err != nil {
		return false, err
	}
	for _, addr := range addrs {
		if ip := net.ParseIP(addr); ip != nil && ip.Equal(monitoredIP) {
			return true, nil
		}
	}
	return false, nil
}

// lookupHost is the resolver ResolveHost uses; a variable so tests can observe the name and
// deadline it is given.
var lookupHost = net.DefaultResolver.LookupHost

// ResolveHost resolves a SENTINEL MONITOR target hostname to its current IPs, within
// resolveTimeout. A fully qualified name gets a trailing dot so the resolver does not first try
// it against every search domain.
func ResolveHost(ctx context.Context, host string) ([]string, error) {
	name := host
	if strings.Contains(name, ".") && !strings.HasSuffix(name, ".") {
		name += "."
	}
	ctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	addrs, err := lookupHost(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrMonitorTargetUnresolved, host, err)
	}
	return addrs, nil
}

func (c *service) SentinelPeers(ctx context.Context, masterGroupName string) (*SentinelPeersResult, error) {
	client := c.createClient()
	if client == nil {
		return &SentinelPeersResult{}, nil
	}
	defer client.Close()

	replicas, err := sentinelInstances(ctx, client, "REPLICAS", masterGroupName)
	if err != nil {
		return nil, err
	}
	sentinels, err := sentinelInstances(ctx, client, "SENTINELS", masterGroupName)
	if err != nil {
		return nil, err
	}
	return &SentinelPeersResult{Replicas: replicas, Sentinels: sentinels}, nil
}

func sentinelInstances(ctx context.Context, client *rediscli.Client, sub, masterGroupName string) ([]SentinelPeer, error) {
	items, err := client.Do(ctx, "SENTINEL", sub, masterGroupName).Slice()
	if err != nil {
		return nil, err
	}
	peers := make([]SentinelPeer, 0, len(items))
	for _, item := range items {
		f := sentinelFields(item)
		peers = append(peers, SentinelPeer{IP: f["ip"], Port: f["port"], Flags: f["flags"]})
	}
	return peers, nil
}

// sentinelFields turns one sentinel instance reply (SENTINEL MASTER, or one element of
// SENTINEL REPLICAS / SENTINELS) into its fields. Sentinel sends a flat key/value array under
// RESP2 and a map under RESP3; a slice command reads only half of a RESP3 map and leaves the rest
// on the connection, so these replies are read generically and both shapes are accepted.
func sentinelFields(reply interface{}) map[string]string {
	fields := map[string]string{}
	switch v := reply.(type) {
	case []interface{}:
		for i := 0; i+1 < len(v); i += 2 {
			key, kok := sentinelString(v[i])
			val, vok := sentinelString(v[i+1])
			if kok && vok {
				fields[key] = val
			}
		}
	case map[interface{}]interface{}:
		for k, val := range v {
			key, kok := sentinelString(k)
			str, vok := sentinelString(val)
			if kok && vok {
				fields[key] = str
			}
		}
	}
	return fields
}

func sentinelString(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	case int64:
		return strconv.FormatInt(s, 10), true
	}
	return "", false
}

func (c *service) IsMaster(ctx context.Context) (bool, error) {
	client := c.createClient()
	if client == nil {
		return false, nil
	}
	defer client.Close()

	result, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}
	return strings.Contains(result, redisRoleMaster), nil
}

func (c *service) GetAttachedReplicaCount(ctx context.Context) (int, error) {
	client := c.createClient()
	if client == nil {
		return 0, nil
	}
	defer client.Close()

	result, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return 0, err
	}

	var count int
	for _, line := range strings.Split(result, "\r\n") {
		if value, found := strings.CutPrefix(line, "connected_slaves:"); found {
			count, err = strconv.Atoi(value)
			if err != nil {
				return 0, err
			}
			return count, nil
		}
	}
	return count, nil
}

// GetClusterInfo get cluster information by checking slot allocation
func (c *service) GetClusterInfo(ctx context.Context) (*ClusterStatus, error) {
	client := c.createClient()
	if client == nil {
		return nil, nil
	}
	defer client.Close()

	slots, err := client.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, err
	}

	status := &ClusterStatus{
		SlotsAssigned: 0,
	}

	for _, slot := range slots {
		if slot.Start <= slot.End && len(slot.Nodes) > 0 {
			status.SlotsAssigned += slot.End - slot.Start + 1
		}
	}

	return status, nil
}
