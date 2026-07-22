package s3client

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/blake-golliher/s3lister/internal/config"
)

func newTestFast(t *testing.T, handler http.HandlerFunc) (*FastClient, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	fc := NewFast(&config.S3Config{
		AccessKey: "AKIDEXAMPLE",
		SecretKey: "secret",
		Endpoint:  srv.URL,
		Region:    "us-east-1",
	}, false, log.New(io.Discard, "", 0))
	return fc, srv
}

func TestFastListPageEndToEnd(t *testing.T) {
	var gotQuery url.Values
	var gotAuth, gotSHA string
	fc, _ := newTestFast(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		gotAuth = r.Header.Get("Authorization")
		gotSHA = r.Header.Get("X-Amz-Content-Sha256")
		if r.URL.Path != "/bench" {
			t.Errorf("path = %q", r.URL.Path)
		}
		io.WriteString(w, listFixture)
	})

	var page ListPage
	q := &ListQuery{
		Prefix:            "data/spaced dir/",
		Delimiter:         "/",
		ContinuationToken: "tok+with/specials=",
		MaxKeys:           1000,
	}
	if err := fc.ListPage(context.Background(), "bench", q, &page); err != nil {
		t.Fatalf("ListPage: %v", err)
	}

	if gotQuery.Get("list-type") != "2" || gotQuery.Get("max-keys") != "1000" {
		t.Errorf("query: %v", gotQuery)
	}
	if gotQuery.Get("prefix") != "data/spaced dir/" {
		t.Errorf("prefix decoded wrong: %q", gotQuery.Get("prefix"))
	}
	if gotQuery.Get("continuation-token") != "tok+with/specials=" {
		t.Errorf("token decoded wrong: %q", gotQuery.Get("continuation-token"))
	}
	if !strings.HasPrefix(gotAuth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/") {
		t.Errorf("authorization header: %q", gotAuth)
	}
	if gotSHA != emptyPayloadSHA {
		t.Errorf("content sha: %q", gotSHA)
	}
	if len(page.Objects) != 3 || !page.IsTruncated {
		t.Errorf("parse: %d objects truncated=%v", len(page.Objects), page.IsTruncated)
	}
}

func TestFastGetTaggingEndToEnd(t *testing.T) {
	fc, _ := newTestFast(t, func(w http.ResponseWriter, r *http.Request) {
		// Path arrives percent-encoded on the wire; the server decodes it.
		if r.URL.Path != "/bench/dir with space/obj+plus.txt" {
			t.Errorf("path = %q (raw %q)", r.URL.Path, r.URL.RawPath)
		}
		if _, ok := r.URL.Query()["tagging"]; !ok {
			t.Errorf("missing ?tagging in %q", r.URL.RawQuery)
		}
		io.WriteString(w, `<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`)
	})

	tags, err := fc.GetTagging(context.Background(), "bench", "dir with space/obj+plus.txt")
	if err != nil {
		t.Fatalf("GetTagging: %v", err)
	}
	if tags["env"] != "prod" {
		t.Errorf("tags: %v", tags)
	}
}

func TestFastErrorStatus(t *testing.T) {
	fc, _ := newTestFast(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `<Error><Code>SlowDown</Code><Message>chill</Message></Error>`)
	})

	var page ListPage
	err := fc.ListPage(context.Background(), "bench", &ListQuery{MaxKeys: 10}, &page)
	if err == nil || !strings.Contains(err.Error(), "SlowDown") || !strings.Contains(err.Error(), "503") {
		t.Errorf("want 503 SlowDown error, got %v", err)
	}

	if _, err := fc.GetTagging(context.Background(), "bench", "k"); err == nil {
		t.Error("tagging on 503 should error")
	}
}

func TestFastBufferReuseAcrossPages(t *testing.T) {
	calls := 0
	fc, _ := newTestFast(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			io.WriteString(w, listFixture)
			return
		}
		io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
	})

	var page ListPage
	q := &ListQuery{MaxKeys: 1000}
	if err := fc.ListPage(context.Background(), "bench", q, &page); err != nil {
		t.Fatal(err)
	}
	if err := fc.ListPage(context.Background(), "bench", q, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 0 || page.IsTruncated {
		t.Errorf("second page leaked first page's state: %+v", page)
	}
}
