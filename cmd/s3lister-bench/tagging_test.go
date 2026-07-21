package main

import (
	"net/url"
	"testing"
)

// TestTaggingInvariants checks the exact-count claims the bench-readme makes
// for -tags: over any window whose size is a multiple of 160, a fifth of the
// objects sit in each tag_count bucket (0-4), each of the 8 keys appears on
// exactly N/4 objects, and each key=value pair on exactly N/16.
func TestTaggingInvariants(t *testing.T) {
	const n = 160 * 100

	tagCounts := make(map[int]int64)
	keyCounts := make(map[string]int64)
	pairCounts := make(map[string]int64)

	table := buildTaggingTable()
	for i := int64(0); i < n; i++ {
		enc := benchTagging(i)
		if enc != table[i%160] {
			t.Fatalf("i=%d: table[%d]=%q but benchTagging=%q", i, i%160, table[i%160], enc)
		}
		vals, err := url.ParseQuery(enc)
		if err != nil {
			t.Fatalf("i=%d: unparseable tagging %q: %v", i, enc, err)
		}
		tagCounts[len(vals)]++
		for k, v := range vals {
			if len(v) != 1 {
				t.Fatalf("i=%d: duplicate tag key %q", i, k)
			}
			keyCounts[k]++
			pairCounts[k+"="+v[0]]++
		}
	}

	for c := 0; c <= 4; c++ {
		if tagCounts[c] != n/5 {
			t.Errorf("tag_count=%d on %d objects, want %d", c, tagCounts[c], n/5)
		}
	}
	if len(keyCounts) != len(benchTagKeys) {
		t.Errorf("saw %d distinct keys, want %d", len(keyCounts), len(benchTagKeys))
	}
	for _, k := range benchTagKeys {
		if keyCounts[k] != n/4 {
			t.Errorf("key %q on %d objects, want %d", k, keyCounts[k], n/4)
		}
	}
	if len(pairCounts) != 32 {
		t.Errorf("saw %d distinct key=value pairs, want 32", len(pairCounts))
	}
	for kv, c := range pairCounts {
		if c != n/16 {
			t.Errorf("pair %q on %d objects, want %d", kv, c, n/16)
		}
	}
}
