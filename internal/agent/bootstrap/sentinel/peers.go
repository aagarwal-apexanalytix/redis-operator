package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	rediscli "github.com/redis/go-redis/v9"
)

// placeholderMasterHost is the address the bootstrap writes into `sentinel monitor` when it
// has no real master to give. The line cannot simply be left out: Sentinel loads every
// per-master directive (down-after-milliseconds, parallel-syncs, failover-timeout, auth-pass)
// AFTER the monitor lines, and each of them is a FATAL config error ("No such master with
// specified name.") when its master was never declared. A sentinel booted on the placeholder
// stays unready (see the sentinel readiness probe in k8sutils) until the operator issues
// SENTINEL MONITOR with the real master.
const placeholderMasterHost = "0.0.0.0"

const (
	// peerQueryTimeout bounds the dial and each read/write against ONE peer sentinel, so a
	// peer that accepts the connection and then never answers cannot stall the init container.
	peerQueryTimeout = 2 * time.Second
	// peerLookupBudget bounds the whole lookup (DNS plus every peer). Past it the bootstrap
	// writes the placeholder, which is exactly what it did before the lookup existed.
	peerLookupBudget = 10 * time.Second
)

var (
	errNoPeerService   = errors.New("peer sentinel service name is unknown")
	errNoPeers         = errors.New("no peer sentinel address resolved")
	errNoUsableAnswer  = errors.New("no peer sentinel reported a usable master")
	errAmbiguousAnswer = errors.New("peer sentinels disagree on the master at the same config epoch")
)

// monitoredMaster is one peer sentinel's view of the master it monitors.
type monitoredMaster struct {
	host        string
	port        string
	configEpoch int64
}

func (m monitoredMaster) address() string {
	return net.JoinHostPort(m.host, m.port)
}

// peerLookup asks the other sentinels of this deployment which master they monitor, so a
// freshly started sentinel can boot on the real master instead of the placeholder.
type peerLookup struct {
	// service is the DNS name that resolves to the peer sentinels: the StatefulSet's headless
	// service. It publishes only Ready pods, and a sentinel is Ready only once it reports a
	// real master, so the peers it returns are the ones worth asking.
	service string
	// port is the sentinel port of every peer.
	port string
	// allowHostname admits a master reported as a hostname. Sentinel accepts a hostname in
	// `sentinel monitor` only with `resolve-hostnames yes`; anything else would make the
	// generated config fail to load.
	allowHostname bool
	resolve       func(ctx context.Context, host string) ([]string, error)
	query         func(ctx context.Context, addr string) (monitoredMaster, error)
}

// discover returns the master the peer sentinels agree on. Answers that are unusable (the
// placeholder, an empty or malformed address) are ignored. Among the usable answers the
// highest config epoch wins, because that is how Sentinel itself orders conflicting views of
// a master: the epoch increases with every failover, so a lower one is an older view. If the
// winning epoch still holds different addresses, the most-reported one wins, and a tie
// returns errAmbiguousAnswer so the caller falls back to the placeholder and leaves the
// decision to the operator.
func (l peerLookup) discover(ctx context.Context) (monitoredMaster, error) {
	if l.service == "" {
		return monitoredMaster{}, errNoPeerService
	}
	addrs, err := l.resolve(ctx, l.service)
	if err != nil {
		return monitoredMaster{}, fmt.Errorf("%w: resolve %s: %v", errNoPeers, l.service, err)
	}
	if len(addrs) == 0 {
		return monitoredMaster{}, fmt.Errorf("%w: %s has no addresses", errNoPeers, l.service)
	}

	var answers []monitoredMaster
	var failures []string
	for _, addr := range addrs {
		if ctx.Err() != nil {
			failures = append(failures, fmt.Sprintf("lookup budget exhausted before %s", addr))
			break
		}
		peer := net.JoinHostPort(addr, l.port)
		m, err := l.query(ctx, peer)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", peer, err))
			continue
		}
		if !l.usable(m) {
			failures = append(failures, fmt.Sprintf("%s: unusable master %q port %q", peer, m.host, m.port))
			continue
		}
		answers = append(answers, m)
	}
	if len(answers) == 0 {
		return monitoredMaster{}, fmt.Errorf("%w (%s)", errNoUsableAnswer, strings.Join(failures, "; "))
	}
	return pickMaster(answers)
}

func (l peerLookup) usable(m monitoredMaster) bool {
	host := strings.TrimSpace(m.host)
	if host == "" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		// The placeholder 0.0.0.0, or its IPv6 form ::.
		if ip.IsUnspecified() {
			return false
		}
	} else if !l.allowHostname {
		return false
	}
	port, err := strconv.Atoi(m.port)
	return err == nil && port > 0 && port <= 65535
}

func pickMaster(answers []monitoredMaster) (monitoredMaster, error) {
	maxEpoch := answers[0].configEpoch
	for _, a := range answers[1:] {
		if a.configEpoch > maxEpoch {
			maxEpoch = a.configEpoch
		}
	}
	votes := map[string]int{}
	byAddr := map[string]monitoredMaster{}
	for _, a := range answers {
		if a.configEpoch != maxEpoch {
			continue
		}
		votes[a.address()]++
		byAddr[a.address()] = a
	}
	best, bestVotes, tied := "", 0, false
	for addr, n := range votes {
		switch {
		case n > bestVotes:
			best, bestVotes, tied = addr, n, false
		case n == bestVotes:
			tied = true
		}
	}
	if tied {
		return monitoredMaster{}, fmt.Errorf("%w (config epoch %d: %v)", errAmbiguousAnswer, maxEpoch, votes)
	}
	return byAddr[best], nil
}

// peerServiceFromFQDN derives the headless service name from this pod's own FQDN
// (<pod>.<headless-service>.<namespace>.svc.<domain>): everything after the pod label.
func peerServiceFromFQDN(fqdn string) string {
	_, service, ok := strings.Cut(strings.TrimSuffix(fqdn, "."), ".")
	if !ok || !strings.Contains(service, ".") {
		return ""
	}
	return service
}

// querySentinelMaster returns a query that asks one sentinel `SENTINEL MASTER <group>`.
func querySentinelMaster(password, masterGroup string, timeout time.Duration) func(context.Context, string) (monitoredMaster, error) {
	return func(ctx context.Context, addr string) (monitoredMaster, error) {
		client := rediscli.NewSentinelClient(&rediscli.Options{
			Addr:            addr,
			Password:        password,
			DialTimeout:     timeout,
			ReadTimeout:     timeout,
			WriteTimeout:    timeout,
			MaxRetries:      -1,
			Protocol:        2,
			DisableIdentity: true,
		})
		defer client.Close()

		info, err := client.Master(ctx, masterGroup).Result()
		if err != nil {
			return monitoredMaster{}, err
		}
		// A sentinel on its very first epoch may not report the field; 0 is its meaning.
		epoch, _ := strconv.ParseInt(info["config-epoch"], 10, 64)
		return monitoredMaster{host: info["ip"], port: info["port"], configEpoch: epoch}, nil
	}
}

// discoverPeerMaster is the production lookup GenerateConfig uses. It is a variable so the
// config tests can replace it without a network.
var discoverPeerMaster = func(masterGroup, sentinelPort, password string, allowHostname bool) (monitoredMaster, error) {
	fqdn, err := fqdnHostname()
	if err != nil {
		return monitoredMaster{}, fmt.Errorf("%w: %v", errNoPeerService, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), peerLookupBudget)
	defer cancel()
	return peerLookup{
		service:       peerServiceFromFQDN(fqdn),
		port:          sentinelPort,
		allowHostname: allowHostname,
		resolve:       net.DefaultResolver.LookupHost,
		query:         querySentinelMaster(password, masterGroup, peerQueryTimeout),
	}.discover(ctx)
}
