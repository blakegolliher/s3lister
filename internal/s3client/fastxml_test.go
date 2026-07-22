package s3client

import (
	"strings"
	"testing"
	"time"
)

const listFixture = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bench</Name>
  <Prefix>data/</Prefix>
  <KeyCount>3</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>1dPXbKUP+u/Yw==</NextContinuationToken>
  <Contents>
    <Key>data/report &amp; summary &lt;v2&gt;.csv</Key>
    <LastModified>2026-07-21T20:15:42.123Z</LastModified>
    <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>
    <Size>1048576</Size>
    <Owner><ID>abc123</ID><DisplayName>blake</DisplayName></Owner>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>data/plain.txt</Key>
    <LastModified>2026-01-02T03:04:05Z</LastModified>
    <ETag>&quot;aabbcc&quot;</ETag>
    <Size>0</Size>
    <StorageClass>GLACIER</StorageClass>
    <ChecksumAlgorithm>SHA256</ChecksumAlgorithm>
  </Contents>
  <Contents>
    <Key>data/weird&#37;name&#x2F;file</Key>
    <LastModified>2026-07-21T20:15:42.999999999Z</LastModified>
    <ETag></ETag>
    <Size>9223372036854775807</Size>
    <StorageClass></StorageClass>
  </Contents>
  <CommonPrefixes><Prefix>data/d000/</Prefix></CommonPrefixes>
  <CommonPrefixes><Prefix>data/d001/</Prefix></CommonPrefixes>
</ListBucketResult>`

func TestParseListPage(t *testing.T) {
	var page ListPage
	if err := parseListPage([]byte(listFixture), &page); err != nil {
		t.Fatalf("parseListPage: %v", err)
	}

	if !page.IsTruncated {
		t.Error("IsTruncated should be true")
	}
	if page.NextToken != "1dPXbKUP+u/Yw==" {
		t.Errorf("NextToken = %q", page.NextToken)
	}
	if len(page.Objects) != 3 {
		t.Fatalf("got %d objects, want 3", len(page.Objects))
	}

	o := page.Objects[0]
	if o.Key != "data/report & summary <v2>.csv" {
		t.Errorf("escaped key round-trip: %q", o.Key)
	}
	if o.ETag != `"d41d8cd98f00b204e9800998ecf8427e"` {
		t.Errorf("etag: %q", o.ETag)
	}
	if o.Size != 1048576 || o.StorageClass != "STANDARD" {
		t.Errorf("size/class: %d %q", o.Size, o.StorageClass)
	}
	want := time.Date(2026, 7, 21, 20, 15, 42, 123_000_000, time.UTC)
	if !o.LastModified.Equal(want) {
		t.Errorf("lastModified: %v want %v", o.LastModified, want)
	}

	// Owner and ChecksumAlgorithm subtrees skipped without disturbing fields.
	if page.Objects[1].Key != "data/plain.txt" || page.Objects[1].StorageClass != "GLACIER" {
		t.Errorf("second object: %+v", page.Objects[1])
	}
	if !page.Objects[1].LastModified.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("no-fraction timestamp: %v", page.Objects[1].LastModified)
	}

	// Numeric character references, max int64, empty leaf elements.
	if page.Objects[2].Key != "data/weird%name/file" {
		t.Errorf("charref key: %q", page.Objects[2].Key)
	}
	if page.Objects[2].Size != 9223372036854775807 {
		t.Errorf("max size: %d", page.Objects[2].Size)
	}
	if page.Objects[2].ETag != "" || page.Objects[2].StorageClass != "" {
		t.Errorf("empty leaves: %+v", page.Objects[2])
	}

	if len(page.CommonPrefixes) != 2 || page.CommonPrefixes[0] != "data/d000/" || page.CommonPrefixes[1] != "data/d001/" {
		t.Errorf("common prefixes: %v", page.CommonPrefixes)
	}
}

func TestParseListPageReuse(t *testing.T) {
	var page ListPage
	if err := parseListPage([]byte(listFixture), &page); err != nil {
		t.Fatal(err)
	}
	empty := `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>`
	if err := parseListPage([]byte(empty), &page); err != nil {
		t.Fatalf("empty page: %v", err)
	}
	if len(page.Objects) != 0 || len(page.CommonPrefixes) != 0 || page.IsTruncated || page.NextToken != "" {
		t.Errorf("stale state after reuse: %+v", page)
	}
}

func TestParseListPageSelfClosingRoot(t *testing.T) {
	var page ListPage
	if err := parseListPage([]byte(`<ListBucketResult/>`), &page); err != nil {
		t.Fatalf("self-closing root: %v", err)
	}
}

func TestParseListPageErrorBody(t *testing.T) {
	body := `<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>nope</Message></Error>`
	var page ListPage
	err := parseListPage([]byte(body), &page)
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("want AccessDenied error, got %v", err)
	}
}

func TestParseListPageTruncatedInput(t *testing.T) {
	var page ListPage
	for _, frag := range []string{
		"",
		"<ListBucketResult><Contents><Key>a",
		"<ListBucketResult><Contents>",
	} {
		if err := parseListPage([]byte(frag), &page); err == nil {
			t.Errorf("fragment %q: want error, got nil", frag)
		}
	}
}

func TestParseTagging(t *testing.T) {
	body := `<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet>
  <Tag><Key>env</Key><Value>prod</Value></Tag>
  <Tag><Key>note</Key><Value>a&amp;b &quot;c&quot;</Value></Tag>
  <Tag><Key>empty</Key><Value></Value></Tag>
</TagSet></Tagging>`
	tags, err := parseTagging([]byte(body))
	if err != nil {
		t.Fatalf("parseTagging: %v", err)
	}
	if len(tags) != 3 || tags["env"] != "prod" || tags["note"] != `a&b "c"` || tags["empty"] != "" {
		t.Errorf("tags: %v", tags)
	}
}

func TestParseTaggingEmpty(t *testing.T) {
	for _, body := range []string{
		`<Tagging><TagSet></TagSet></Tagging>`,
		`<Tagging><TagSet/></Tagging>`,
		`<Tagging/>`,
	} {
		tags, err := parseTagging([]byte(body))
		if err != nil {
			t.Fatalf("%q: %v", body, err)
		}
		if tags == nil || len(tags) != 0 {
			t.Errorf("%q: want non-nil empty map, got %v", body, tags)
		}
	}
}

func TestParseS3Time(t *testing.T) {
	cases := map[string]time.Time{
		"2026-07-21T20:00:00.000Z":       time.Date(2026, 7, 21, 20, 0, 0, 0, time.UTC),
		"2026-07-21T20:00:00Z":           time.Date(2026, 7, 21, 20, 0, 0, 0, time.UTC),
		"2026-07-21T20:00:00.123456789Z": time.Date(2026, 7, 21, 20, 0, 0, 123456789, time.UTC),
		"2026-07-21T20:00:00+00:00":      time.Date(2026, 7, 21, 20, 0, 0, 0, time.UTC), // fallback path
	}
	for in, want := range cases {
		got, err := parseS3Time([]byte(in))
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if !got.Equal(want) {
			t.Errorf("%s: got %v want %v", in, got, want)
		}
	}
	if _, err := parseS3Time([]byte("not-a-time")); err == nil {
		t.Error("garbage timestamp should error")
	}
}
