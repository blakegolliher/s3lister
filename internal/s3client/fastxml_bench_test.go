package s3client

import (
	"encoding/xml"
	"fmt"
	"strings"
	"testing"
	"time"
)

// benchPage is a realistic full ListObjectsV2 page: 1000 keys shaped like the
// benchmark layout, ~90KB of XML.
var benchPage = func() []byte {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bench-2b</Name><Prefix>data/d000/s000/</Prefix><KeyCount>1000</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>true</IsTruncated><NextContinuationToken>data/d000/s000/obj-000000000999.log</NextContinuationToken>`)
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&sb, "<Contents><Key>data/d000/s000/obj-%012d.log</Key><LastModified>2026-07-21T20:15:42.123Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>0</Size><StorageClass>STANDARD</StorageClass></Contents>", i)
	}
	sb.WriteString(`</ListBucketResult>`)
	return []byte(sb.String())
}()

func BenchmarkParseListPageFast(b *testing.B) {
	var page ListPage
	b.SetBytes(int64(len(benchPage)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := parseListPage(benchPage, &page); err != nil {
			b.Fatal(err)
		}
	}
	if len(page.Objects) != 1000 {
		b.Fatalf("parsed %d objects", len(page.Objects))
	}
}

// The encoding/xml equivalent of what the AWS SDK's deserializer does.
type xmlObject struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
}

type xmlListResult struct {
	IsTruncated           bool        `xml:"IsTruncated"`
	NextContinuationToken string      `xml:"NextContinuationToken"`
	Contents              []xmlObject `xml:"Contents"`
	CommonPrefixes        []struct {
		Prefix string `xml:"Prefix"`
	} `xml:"CommonPrefixes"`
}

func BenchmarkParseListPageEncodingXML(b *testing.B) {
	b.SetBytes(int64(len(benchPage)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var res xmlListResult
		if err := xml.Unmarshal(benchPage, &res); err != nil {
			b.Fatal(err)
		}
		if len(res.Contents) != 1000 {
			b.Fatalf("parsed %d objects", len(res.Contents))
		}
	}
}
