package s3client

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

var errNoHost = errors.New("no such host")

// fakeDNS stands in for both the system resolver and authoritative
// nameservers. The zero value knows no names.
type fakeDNS struct {
	mu      sync.Mutex
	system  func(host string) []string            // system resolver answers
	ns      map[string][]string                   // zone -> nameserver hostnames
	servers map[string]func(host string) []string // "ip:53" -> answers
	queries map[string]int                        // server ("" = system) -> lookups
	nsAsked []string
}

func (f *fakeDNS) lookup(ctx context.Context, server, host string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.queries == nil {
		f.queries = make(map[string]int)
	}
	f.queries[server]++
	answer := f.system
	if server != "" {
		answer = f.servers[server]
	}
	if answer == nil {
		return nil, errNoHost
	}
	if ips := answer(host); len(ips) > 0 {
		return ips, nil
	}
	return nil, errNoHost
}

func (f *fakeDNS) lookupNS(ctx context.Context, zone string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nsAsked = append(f.nsAsked, zone)
	if hosts := f.ns[zone]; len(hosts) > 0 {
		return hosts, nil
	}
	return nil, errNoHost
}

func (f *fakeDNS) count(server string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queries[server]
}

// rotating answers one address per query, cycling through ips.
func rotating(ips ...string) func(string) []string {
	var mu sync.Mutex
	n := 0
	return func(string) []string {
		mu.Lock()
		defer mu.Unlock()
		n++
		return []string{ips[(n-1)%len(ips)]}
	}
}

func fixed(ips ...string) func(string) []string {
	return func(string) []string { return ips }
}

func testDialer(f *fakeDNS) *roundRobinDialer {
	d := newRoundRobinDialer(&net.Dialer{Timeout: time.Second}, log.New(io.Discard, "", 0))
	d.lookup = f.lookup
	d.lookupNS = f.lookupNS
	return d
}

func TestDiscoverSamplesAuthoritativeBehindCachingResolver(t *testing.T) {
	// The system resolver has cached one rotating answer; the delegated
	// nameserver rotates through the whole pool.
	pool := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6"}
	f := &fakeDNS{
		system: func(host string) []string {
			if host == "ns.lab.example" {
				return []string{"10.9.9.9"}
			}
			return []string{"10.0.0.5"}
		},
		ns:      map[string][]string{"lab.example": {"ns.lab.example"}},
		servers: map[string]func(string) []string{"10.9.9.9:53": rotating(pool...)},
	}
	d := testDialer(f)

	ips, _ := d.discover(context.Background(), "s3.lab.example")
	if !slices.Equal(ips, pool) {
		t.Fatalf("expected the whole pool, sorted, got %v", ips)
	}
	// Four quiet rotations after the last new address, and no more.
	if n := f.count("10.9.9.9:53"); n != len(pool)+quietCycles*len(pool) {
		t.Fatalf("expected %d authoritative queries, got %d", len(pool)+quietCycles*len(pool), n)
	}
	if !slices.Equal(f.nsAsked, []string{"s3.lab.example", "lab.example"}) {
		t.Fatalf("expected NS walk to stop at the first zone found, asked %v", f.nsAsked)
	}
}

func TestDiscoverStopsQuicklyOnFullAnswers(t *testing.T) {
	// A static record with every address in each answer has nothing to
	// rotate: one quiet run of minQuiet queries settles it.
	pool := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6", "10.0.0.7", "10.0.0.8",
		"10.0.0.9", "10.0.0.10", "10.0.0.11", "10.0.0.12", "10.0.0.13", "10.0.0.14", "10.0.0.15", "10.0.0.16"}
	f := &fakeDNS{system: fixed(pool...)}
	d := testDialer(f)

	ips, _ := d.discover(context.Background(), "s3.example")
	if len(ips) != len(pool) {
		t.Fatalf("expected %d addresses, got %v", len(pool), ips)
	}
	if n := f.count(""); n != 1+minQuiet {
		t.Fatalf("expected %d system lookups, got %d", 1+minQuiet, n)
	}
}

func TestDiscoverSkipsUnreachableNameserver(t *testing.T) {
	f := &fakeDNS{
		system: func(host string) []string {
			if host == "ns.example" {
				return []string{"10.9.9.1", "10.9.9.2"}
			}
			return []string{"10.0.0.1"}
		},
		ns: map[string][]string{"example": {"ns.example"}, "s3.example": {"ns.example"}},
		// 10.9.9.1 never answers.
		servers: map[string]func(string) []string{"10.9.9.2:53": rotating("10.0.0.1", "10.0.0.2")},
	}
	d := testDialer(f)

	ips, how := d.discover(context.Background(), "s3.example")
	if !slices.Equal(ips, []string{"10.0.0.1", "10.0.0.2"}) {
		t.Fatalf("expected the second nameserver's pool, got %v (%s)", ips, how)
	}
	if f.count("10.9.9.1:53") != 1 {
		t.Fatalf("expected one failed query to the unreachable nameserver, got %d", f.count("10.9.9.1:53"))
	}
	if slices.Contains(f.nsAsked, "example") {
		t.Fatalf("top-level domain must never be asked for NS records, asked %v", f.nsAsked)
	}
}

func TestDiscoverWithoutNameserverUsesSystemResolver(t *testing.T) {
	f := &fakeDNS{system: rotating("10.0.0.1", "10.0.0.2", "10.0.0.3")}
	d := testDialer(f)

	ips, _ := d.discover(context.Background(), "s3.example")
	if !slices.Equal(ips, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}) {
		t.Fatalf("expected the system resolver's rotation, got %v", ips)
	}
}

func TestResolveCachesWithinRefreshInterval(t *testing.T) {
	f := &fakeDNS{system: fixed("10.0.0.2", "10.0.0.1")}
	d := testDialer(f)

	first := d.resolve(context.Background(), "s3.example")
	lookups := f.count("")
	second := d.resolve(context.Background(), "s3.example")

	if f.count("") != lookups {
		t.Fatalf("expected no lookups within the refresh interval, got %d more", f.count("")-lookups)
	}
	if !slices.Equal(first, []string{"10.0.0.1", "10.0.0.2"}) || !slices.Equal(second, first) {
		t.Fatalf("expected sorted, cached addresses, got %v then %v", first, second)
	}
}

func TestResolveRefreshesInBackground(t *testing.T) {
	answer := "10.0.0.1"
	release := make(chan struct{})
	var mu sync.Mutex
	f := &fakeDNS{system: func(string) []string {
		mu.Lock()
		defer mu.Unlock()
		return []string{answer}
	}}
	d := testDialer(f)
	if got := d.resolve(context.Background(), "s3.example"); !slices.Equal(got, []string{"10.0.0.1"}) {
		t.Fatalf("expected initial pool, got %v", got)
	}

	// A stale pool is served as-is while the refresh runs.
	mu.Lock()
	answer = "10.0.0.2"
	mu.Unlock()
	inner := f.system
	f.system = func(h string) []string { <-release; return inner(h) }
	d.mu.Lock()
	d.fetched = time.Now().Add(-2 * dnsRefreshInterval)
	d.mu.Unlock()

	if got := d.resolve(context.Background(), "s3.example"); !slices.Equal(got, []string{"10.0.0.1"}) {
		t.Fatalf("expected stale pool while refreshing, got %v", got)
	}
	close(release)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := d.resolve(context.Background(), "s3.example"); slices.Equal(got, []string{"10.0.0.2"}) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("background refresh never installed the new pool")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRefreshKeepsStaleAddressesOnFailure(t *testing.T) {
	f := &fakeDNS{system: fixed("10.0.0.1")}
	d := testDialer(f)
	if got := d.resolve(context.Background(), "s3.example"); len(got) != 1 {
		t.Fatalf("expected 1 address, got %v", got)
	}

	f.system = nil // DNS down
	d.refresh("s3.example")
	if got := d.resolve(context.Background(), "s3.example"); !slices.Equal(got, []string{"10.0.0.1"}) {
		t.Fatalf("expected stale address to survive DNS failure, got %v", got)
	}
}

func TestDialRotatesAcrossAddresses(t *testing.T) {
	// Two listeners on one port, standing in for two VIPs.
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()
	port := strconv.Itoa(l1.Addr().(*net.TCPAddr).Port)
	l2, err := net.Listen("tcp", net.JoinHostPort("127.0.0.2", port))
	if err != nil {
		t.Skipf("127.0.0.2 not usable here: %v", err)
	}
	defer l2.Close()

	accepted := make(chan string, 8)
	for _, l := range []net.Listener{l1, l2} {
		go func(l net.Listener) {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				accepted <- c.LocalAddr().(*net.TCPAddr).IP.String()
				c.Close()
			}
		}(l)
	}

	d := testDialer(&fakeDNS{system: fixed("127.0.0.1", "127.0.0.2")})
	for i := 0; i < 4; i++ {
		conn, err := d.DialContext(context.Background(), "tcp", net.JoinHostPort("s3.example", port))
		if err != nil {
			t.Fatalf("dial %d failed: %v", i, err)
		}
		conn.Close()
	}

	counts := map[string]int{}
	for i := 0; i < 4; i++ {
		counts[<-accepted]++
	}
	if counts["127.0.0.1"] != 2 || counts["127.0.0.2"] != 2 {
		t.Fatalf("expected connections split evenly across both addresses, got %v", counts)
	}
}

func TestDialLiteralIPBypassesResolution(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	f := &fakeDNS{}
	d := testDialer(f)
	conn, err := d.DialContext(context.Background(), "tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("literal IP dial failed: %v", err)
	}
	conn.Close()
	if f.count("") != 0 || len(f.nsAsked) != 0 {
		t.Fatal("literal IP endpoints must not be resolved")
	}
}
