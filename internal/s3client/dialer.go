package s3client

import (
	"context"
	"log"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// dnsRefreshInterval is how often the endpoint hostname is re-resolved, so
// VIPs added or removed from DNS are picked up mid-run.
const dnsRefreshInterval = 30 * time.Second

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

	// lookup resolves a hostname to its addresses; overridable in tests.
	lookup func(ctx context.Context, host string) ([]string, error)

	mu      sync.Mutex
	host    string
	ips     []string
	fetched time.Time

	next atomic.Uint64
}

func newRoundRobinDialer(dialer *net.Dialer, logger *log.Logger) *roundRobinDialer {
	return &roundRobinDialer{
		dialer: dialer,
		logger: logger,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			if err != nil {
				return nil, err
			}
			ips := make([]string, 0, len(addrs))
			for _, a := range addrs {
				ips = append(ips, a.Unmap().String())
			}
			return ips, nil
		},
	}
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

// resolve returns the cached address list for host, refreshing it from DNS
// every dnsRefreshInterval. On resolution failure a stale list is better
// than none.
func (d *roundRobinDialer) resolve(ctx context.Context, host string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.host == host && len(d.ips) > 0 && time.Since(d.fetched) < dnsRefreshInterval {
		return d.ips
	}

	ips, err := d.lookup(ctx, host)
	if err != nil || len(ips) == 0 {
		if d.host == host {
			return d.ips
		}
		return nil
	}
	// A stable order keeps the rotation even regardless of how DNS shuffles
	// its answers between refreshes.
	sort.Strings(ips)

	if d.host != host || !slices.Equal(ips, d.ips) {
		d.logger.Printf("[s3] endpoint %s resolves to %d address(es): %s",
			host, len(ips), strings.Join(ips, ", "))
	}
	d.host = host
	d.ips = ips
	d.fetched = time.Now()
	return d.ips
}
