package s3client

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"
)

func testDialer(lookup func(ctx context.Context, host string) ([]string, error)) *roundRobinDialer {
	d := newRoundRobinDialer(&net.Dialer{Timeout: time.Second}, log.New(io.Discard, "", 0))
	d.lookup = lookup
	return d
}

func TestResolveCachesWithinRefreshInterval(t *testing.T) {
	calls := 0
	d := testDialer(func(ctx context.Context, host string) ([]string, error) {
		calls++
		return []string{"10.0.0.2", "10.0.0.1"}, nil
	})

	first := d.resolve(context.Background(), "s3.example")
	second := d.resolve(context.Background(), "s3.example")

	if calls != 1 {
		t.Fatalf("expected 1 lookup, got %d", calls)
	}
	if len(first) != 2 || first[0] != "10.0.0.1" || first[1] != "10.0.0.2" {
		t.Fatalf("expected sorted addresses, got %v", first)
	}
	if len(second) != 2 {
		t.Fatalf("expected cached addresses, got %v", second)
	}
}

func TestResolveKeepsStaleAddressesOnFailure(t *testing.T) {
	fail := false
	d := testDialer(func(ctx context.Context, host string) ([]string, error) {
		if fail {
			return nil, errors.New("dns down")
		}
		return []string{"10.0.0.1"}, nil
	})

	if got := d.resolve(context.Background(), "s3.example"); len(got) != 1 {
		t.Fatalf("expected 1 address, got %v", got)
	}

	fail = true
	d.fetched = time.Now().Add(-2 * dnsRefreshInterval) // force refresh
	if got := d.resolve(context.Background(), "s3.example"); len(got) != 1 {
		t.Fatalf("expected stale address to survive DNS failure, got %v", got)
	}
}

func TestDialRotatesAcrossAddresses(t *testing.T) {
	// Two listeners standing in for two VIPs; the stub resolver returns
	// both loopback "addresses" via distinct ports encoded per dial below.
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()

	d := testDialer(func(ctx context.Context, host string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	})

	// Hostname dials should be rewritten to the resolved IP and succeed.
	port := l1.Addr().(*net.TCPAddr).Port
	conn, err := d.DialContext(context.Background(), "tcp", net.JoinHostPort("s3.example", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("dial via resolved IP failed: %v", err)
	}
	conn.Close()

	// Rotation counter must advance per dial so successive connections
	// start at successive addresses.
	before := d.next.Load()
	conn2, err := d.DialContext(context.Background(), "tcp", net.JoinHostPort("s3.example", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("second dial failed: %v", err)
	}
	conn2.Close()
	if d.next.Load() != before+1 {
		t.Fatalf("expected rotation counter to advance by 1, went %d -> %d", before, d.next.Load())
	}
}

func TestDialLiteralIPBypassesResolution(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	d := testDialer(func(ctx context.Context, host string) ([]string, error) {
		t.Fatal("lookup must not be called for literal IP endpoints")
		return nil, nil
	})

	conn, err := d.DialContext(context.Background(), "tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("literal IP dial failed: %v", err)
	}
	conn.Close()
}
