package s3client

import (
	"context"
	"fmt"
	"log"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// dnsRefreshInterval is how often the endpoint's address pool is
	// rediscovered, so VIPs added or removed from DNS are picked up mid-run.
	dnsRefreshInterval = 30 * time.Second

	// discoveryTimeout bounds one whole discovery, queryTimeout each DNS
	// query within it.
	discoveryTimeout = 10 * time.Second
	queryTimeout     = 2 * time.Second

	// A DNS server is sampled until quietCycles full rotations of the pool
	// found so far (and at least minQuiet queries) in a row turn up no new
	// address, or maxQueries is reached.
	quietCycles = 4
	minQuiet    = 8
	maxQueries  = 256
)

// roundRobinDialer spreads new connections evenly across every address the
// endpoint hostname resolves to. Scale-out S3 front ends publish one VIP per
// node behind a single DNS name; Go's default dialer sorts resolved addresses
// per RFC 6724 and always dials the first, which collapses that whole pool
// onto a single IP no matter how DNS rotates its answers. This dialer rotates
// the dial target per connection and falls through to the next address if one
// refuses the connection.
//
// Only the TCP dial address is rewritten — the HTTP Host header and TLS SNI
// still carry the hostname, so path-style addressing and certificate
// verification behave exactly as before.
type roundRobinDialer struct {
	dialer *net.Dialer
	logger *log.Logger

	// lookup resolves host through the system resolver when server is "",
	// or by querying server (an ip:port) directly. lookupNS returns a
	// zone's nameserver hostnames. Both are overridable in tests.
	lookup   func(ctx context.Context, server, host string) ([]string, error)
	lookupNS func(ctx context.Context, zone string) ([]string, error)

	mu         sync.Mutex
	host       string
	ips        []string
	fetched    time.Time
	refreshing bool

	next atomic.Uint64
}

func newRoundRobinDialer(dialer *net.Dialer, logger *log.Logger) *roundRobinDialer {
	return &roundRobinDialer{
		dialer:   dialer,
		logger:   logger,
		lookup:   lookupAt,
		lookupNS: lookupNS,
	}
}

func lookupAt(ctx context.Context, server, host string) ([]string, error) {
	r := net.DefaultResolver
	if server != "" {
		r = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, server)
			},
		}
		// Rooted, so resolv.conf search domains are never appended.
		if !strings.HasSuffix(host, ".") {
			host += "."
		}
	}
	addrs, err := r.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	ips := make([]string, 0, len(addrs))
	for _, a := range addrs {
		ips = append(ips, a.Unmap().String())
	}
	return ips, nil
}

func lookupNS(ctx context.Context, zone string) ([]string, error) {
	nss, err := net.DefaultResolver.LookupNS(ctx, zone)
	if err != nil {
		return nil, err
	}
	hosts := make([]string, 0, len(nss))
	for _, ns := range nss {
		hosts = append(hosts, ns.Host)
	}
	return hosts, nil
}

func (d *roundRobinDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) != nil {
		// Malformed (let the dialer produce the error) or a literal IP
		// endpoint — nothing to discover.
		return d.dialer.DialContext(ctx, network, addr)
	}

	ips := d.resolve(ctx, host)
	if len(ips) == 0 {
		return d.dialer.DialContext(ctx, network, addr)
	}

	start := int(d.next.Add(1) - 1)
	var firstErr error
	for i := 0; i < len(ips); i++ {
		ip := ips[(start+i)%len(ips)]
		conn, err := d.dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			return conn, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return nil, firstErr
}

// resolve returns host's address pool. The first call discovers it, and
// concurrent dials wait for that one discovery, so the startup burst of
// connections spreads across the whole pool. After that the pool is
// rediscovered in the background every dnsRefreshInterval while dials keep
// using the current one.
func (d *roundRobinDialer) resolve(ctx context.Context, host string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	switch {
	case d.host != host:
		d.host, d.ips = host, nil
		// One cancelled dial must not cut short the discovery every
		// waiting dial will use.
		d.store(d.discover(context.WithoutCancel(ctx), host))
	case time.Since(d.fetched) >= dnsRefreshInterval && !d.refreshing:
		d.refreshing = true
		go d.refresh(host)
	}
	return d.ips
}

func (d *roundRobinDialer) refresh(host string) {
	ips, how := d.discover(context.Background(), host)

	d.mu.Lock()
	defer d.mu.Unlock()
	d.refreshing = false
	if d.host == host {
		d.store(ips, how)
	}
}

// store installs a discovered pool for d.host, logging it when it changes.
// A discovery that found nothing keeps the current pool: a stale list is
// better than none.
func (d *roundRobinDialer) store(ips []string, how string) {
	d.fetched = time.Now()
	if len(ips) == 0 {
		d.logger.Printf("[s3] endpoint %s: DNS discovery found no addresses (%s); keeping %d known address(es)",
			d.host, how, len(d.ips))
		return
	}
	if slices.Equal(ips, d.ips) {
		return
	}
	d.logger.Printf("[s3] endpoint %s resolves to %d address(es): %s (%s)",
		d.host, len(ips), strings.Join(ips, ", "), how)
	if len(ips) == 1 {
		d.logger.Printf("[s3] only one address found for %s: every connection will go to %s", d.host, ips[0])
	}
	d.ips = ips
}

// discover finds every address host publishes, returned sorted, along with
// a note on where they came from. The system resolver alone is not enough:
// load-balancing DNS servers (VAST's among them) answer each query with one
// VIP and rotate it across queries, but caching resolvers in between —
// systemd-resolved, site DNS — hand that one answer back to every query,
// often well past its TTL, so every connection lands on one VIP. So after
// sampling the system resolver, discover finds the zone's authoritative
// nameserver and samples it directly until its rotation stops turning up
// new addresses.
func (d *roundRobinDialer) discover(ctx context.Context, host string) ([]string, string) {
	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()

	seen := make(map[string]struct{})
	found, queries := d.sample(ctx, "", host, seen)
	notes := []string{fmt.Sprintf("system resolver: %d in %d queries", found, queries)}

	servers := d.nameservers(ctx, host)
	if len(servers) == 0 {
		notes = append(notes, "no authoritative nameserver found")
	}
	for _, server := range servers {
		found, queries := d.sample(ctx, server, host, seen)
		if queries == 0 {
			notes = append(notes, fmt.Sprintf("nameserver %s: no answer", server))
			continue
		}
		notes = append(notes, fmt.Sprintf("nameserver %s: %d in %d queries", server, found, queries))
		break
	}

	ips := make([]string, 0, len(seen))
	for ip := range seen {
		ips = append(ips, ip)
	}
	// A stable order keeps the rotation even regardless of the order
	// addresses were discovered in.
	slices.Sort(ips)
	return ips, strings.Join(notes, "; ")
}

// sample queries server (the system resolver if "") for host repeatedly,
// adding every address to seen, until the answers stop turning up new ones.
// It returns how many distinct addresses this server gave and how many of
// its queries were answered, stopping at the first that is not.
func (d *roundRobinDialer) sample(ctx context.Context, server, host string, seen map[string]struct{}) (found, answered int) {
	mine := make(map[string]struct{})
	minAnswer, quiet := 0, 0
	for answered < maxQueries {
		qctx, cancel := context.WithTimeout(ctx, queryTimeout)
		ips, err := d.lookup(qctx, server, host)
		cancel()
		if err != nil || len(ips) == 0 {
			break
		}
		answered++

		grew := false
		for _, ip := range ips {
			if _, ok := mine[ip]; !ok {
				mine[ip] = struct{}{}
				seen[ip] = struct{}{}
				grew = true
			}
		}
		if minAnswer == 0 || len(ips) < minAnswer {
			minAnswer = len(ips)
		}
		if grew {
			quiet = 0
			continue
		}
		// One rotation is the queries it takes to see the pool once: a
		// server answering with every address needs one, one answering
		// with a single rotating address needs as many as there are.
		rotation := (len(mine) + minAnswer - 1) / minAnswer
		if quiet++; quiet >= max(minQuiet, quietCycles*rotation) {
			break
		}
	}
	return len(mine), answered
}

// nameservers returns ip:53 addresses for the nameservers of the closest
// zone enclosing host, found by asking for NS records at host and then at
// each parent domain in turn. Top-level domains are never asked.
func (d *roundRobinDialer) nameservers(ctx context.Context, host string) []string {
	for name := strings.TrimSuffix(host, "."); strings.Contains(name, "."); _, name, _ = strings.Cut(name, ".") {
		qctx, cancel := context.WithTimeout(ctx, queryTimeout)
		nss, err := d.lookupNS(qctx, name)
		cancel()
		if err != nil || len(nss) == 0 {
			continue
		}
		var servers []string
		for _, ns := range nss {
			qctx, cancel := context.WithTimeout(ctx, queryTimeout)
			ips, _ := d.lookup(qctx, "", ns)
			cancel()
			for _, ip := range ips {
				servers = append(servers, net.JoinHostPort(ip, "53"))
			}
		}
		return servers
	}
	return nil
}
