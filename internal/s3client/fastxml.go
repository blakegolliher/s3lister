package s3client

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// This file is a purpose-built parser for the two S3 XML response shapes on
// the scan hot path: ListObjectsV2 and GetObjectTagging. encoding/xml costs
// more CPU than everything else in the listing pipeline combined (it is
// reflection-driven and allocates per token); at hundreds of thousands of
// objects per second that overhead dominates the client. This parser scans
// bytes directly, allocates only the strings that outlive the response
// buffer, and reuses everything else across pages.

// ListedObject is one object from a ListObjectsV2 page.
type ListedObject struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	StorageClass string
}

// ListPage holds one parsed ListObjectsV2 response. Reuse the same ListPage
// across calls: its slices and body buffer are recycled, so a steady-state
// pagination loop allocates only the per-object strings.
type ListPage struct {
	Objects        []ListedObject
	CommonPrefixes []string
	IsTruncated    bool
	NextToken      string

	body []byte // reused response-body buffer
}

// xmlScan is a minimal pull scanner over a complete XML document held in
// memory. It understands exactly what S3 responses contain: elements,
// attributes (skipped), text, XML declarations and comments (skipped). No
// namespaces, CDATA, or DTDs — S3 emits none of those.
type xmlScan struct {
	b []byte
	p int
}

var errTruncatedXML = fmt.Errorf("truncated XML response")

// nextTag advances to the next element tag. closing marks </name>; self marks
// <name/>. ok is false at end of input.
func (s *xmlScan) nextTag() (name []byte, closing, self, ok bool) {
	for {
		i := bytes.IndexByte(s.b[s.p:], '<')
		if i < 0 {
			return nil, false, false, false
		}
		s.p += i + 1
		if s.p >= len(s.b) {
			return nil, false, false, false
		}
		c := s.b[s.p]
		if c == '?' || c == '!' {
			// XML declaration / comment / doctype: skip to '>'.
			j := bytes.IndexByte(s.b[s.p:], '>')
			if j < 0 {
				return nil, false, false, false
			}
			s.p += j + 1
			continue
		}
		if c == '/' {
			closing = true
			s.p++
		}
		start := s.p
		for s.p < len(s.b) {
			c := s.b[s.p]
			if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '>' || c == '/' {
				break
			}
			s.p++
		}
		name = s.b[start:s.p]
		j := bytes.IndexByte(s.b[s.p:], '>')
		if j < 0 {
			return nil, false, false, false
		}
		if !closing && s.b[s.p+j-1] == '/' && j+s.p > start {
			self = true
		}
		s.p += j + 1
		return name, closing, self, true
	}
}

// leaf returns the text content of a leaf element whose open tag was just
// consumed, and consumes its closing tag.
func (s *xmlScan) leaf() []byte {
	i := bytes.IndexByte(s.b[s.p:], '<')
	if i < 0 {
		t := s.b[s.p:]
		s.p = len(s.b)
		return t
	}
	t := s.b[s.p : s.p+i]
	s.p += i
	s.nextTag() // the closing tag
	return t
}

// skipElement consumes the subtree of an element whose open tag was just
// consumed, up to and including its matching close.
func (s *xmlScan) skipElement() {
	depth := 1
	for depth > 0 {
		_, closing, self, ok := s.nextTag()
		if !ok {
			return
		}
		if self {
			continue
		}
		if closing {
			depth--
		} else {
			depth++
		}
	}
}

// unescape decodes the XML entities S3 can emit. The fast path — no '&' in
// the input — is a single scan plus one string allocation.
func unescape(b []byte) string {
	if bytes.IndexByte(b, '&') < 0 {
		return string(b)
	}
	var sb strings.Builder
	sb.Grow(len(b))
	for i := 0; i < len(b); {
		c := b[i]
		if c != '&' {
			sb.WriteByte(c)
			i++
			continue
		}
		j := bytes.IndexByte(b[i:], ';')
		if j < 0 {
			sb.WriteByte(c)
			i++
			continue
		}
		ent := b[i+1 : i+j]
		switch {
		case bytes.Equal(ent, []byte("amp")):
			sb.WriteByte('&')
		case bytes.Equal(ent, []byte("lt")):
			sb.WriteByte('<')
		case bytes.Equal(ent, []byte("gt")):
			sb.WriteByte('>')
		case bytes.Equal(ent, []byte("quot")):
			sb.WriteByte('"')
		case bytes.Equal(ent, []byte("apos")):
			sb.WriteByte('\'')
		case len(ent) > 1 && ent[0] == '#':
			var n int64
			var err error
			if ent[1] == 'x' || ent[1] == 'X' {
				n, err = strconv.ParseInt(string(ent[2:]), 16, 32)
			} else {
				n, err = strconv.ParseInt(string(ent[1:]), 10, 32)
			}
			if err == nil {
				sb.WriteRune(rune(n))
			} else {
				sb.Write(b[i : i+j+1])
			}
		default:
			sb.Write(b[i : i+j+1])
		}
		i += j + 1
	}
	return sb.String()
}

// parseS3Time parses S3's timestamp format (2026-07-21T20:00:00.000Z) with a
// fixed-layout fast path and a time.Parse fallback for anything unusual.
func parseS3Time(b []byte) (time.Time, error) {
	if len(b) >= 20 && b[4] == '-' && b[7] == '-' && b[10] == 'T' && b[13] == ':' && b[16] == ':' {
		year, ok1 := atoiN(b[0:4])
		month, ok2 := atoiN(b[5:7])
		day, ok3 := atoiN(b[8:10])
		hour, ok4 := atoiN(b[11:13])
		min, ok5 := atoiN(b[14:16])
		sec, ok6 := atoiN(b[17:19])
		if ok1 && ok2 && ok3 && ok4 && ok5 && ok6 {
			nsec := 0
			p := 19
			if b[p] == '.' {
				p++
				mult := 100_000_000
				for p < len(b) && b[p] >= '0' && b[p] <= '9' {
					nsec += int(b[p]-'0') * mult
					mult /= 10
					p++
				}
			}
			if p == len(b)-1 && b[p] == 'Z' {
				return time.Date(year, time.Month(month), day, hour, min, sec, nsec, time.UTC), nil
			}
		}
	}
	return time.Parse(time.RFC3339Nano, string(b))
}

// atoiN parses a small all-digit byte slice.
func atoiN(b []byte) (int, bool) {
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

func atoi64(b []byte) int64 {
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

// internStorageClass returns a shared string for the common storage classes
// so a billion-object scan doesn't allocate a billion copies of "STANDARD".
func internStorageClass(b []byte) string {
	switch {
	case len(b) == 0:
		return ""
	case bytes.Equal(b, []byte("STANDARD")):
		return "STANDARD"
	case bytes.Equal(b, []byte("STANDARD_IA")):
		return "STANDARD_IA"
	case bytes.Equal(b, []byte("GLACIER")):
		return "GLACIER"
	case bytes.Equal(b, []byte("DEEP_ARCHIVE")):
		return "DEEP_ARCHIVE"
	case bytes.Equal(b, []byte("INTELLIGENT_TIERING")):
		return "INTELLIGENT_TIERING"
	case bytes.Equal(b, []byte("REDUCED_REDUNDANCY")):
		return "REDUCED_REDUNDANCY"
	case bytes.Equal(b, []byte("GLACIER_IR")):
		return "GLACIER_IR"
	default:
		return string(b)
	}
}

// parseListPage parses a ListObjectsV2 response into page, reusing page's
// slices. Unknown elements (Owner, ChecksumAlgorithm, ...) are skipped.
func parseListPage(body []byte, page *ListPage) error {
	page.Objects = page.Objects[:0]
	page.CommonPrefixes = page.CommonPrefixes[:0]
	page.IsTruncated = false
	page.NextToken = ""

	s := xmlScan{b: body}
	name, closing, self, ok := s.nextTag()
	if !ok || closing {
		return errTruncatedXML
	}
	if !bytes.Equal(name, []byte("ListBucketResult")) {
		return s3BodyError(body)
	}
	if self {
		return nil
	}

	for {
		name, closing, self, ok = s.nextTag()
		if !ok {
			return errTruncatedXML
		}
		if closing {
			// Only the root close can appear at this depth.
			return nil
		}
		switch {
		case bytes.Equal(name, []byte("Contents")):
			if self {
				continue
			}
			var obj ListedObject
			for {
				n2, c2, self2, ok2 := s.nextTag()
				if !ok2 {
					return errTruncatedXML
				}
				if c2 { // </Contents>
					break
				}
				switch {
				case bytes.Equal(n2, []byte("Key")):
					if !self2 {
						obj.Key = unescape(s.leaf())
					}
				case bytes.Equal(n2, []byte("LastModified")):
					if !self2 {
						t, err := parseS3Time(s.leaf())
						if err != nil {
							return fmt.Errorf("bad LastModified: %w", err)
						}
						obj.LastModified = t
					}
				case bytes.Equal(n2, []byte("ETag")):
					if !self2 {
						obj.ETag = unescape(s.leaf())
					}
				case bytes.Equal(n2, []byte("Size")):
					if !self2 {
						obj.Size = atoi64(s.leaf())
					}
				case bytes.Equal(n2, []byte("StorageClass")):
					if !self2 {
						obj.StorageClass = internStorageClass(s.leaf())
					}
				default:
					if !self2 {
						s.skipElement()
					}
				}
			}
			if obj.Key != "" {
				page.Objects = append(page.Objects, obj)
			}
		case bytes.Equal(name, []byte("CommonPrefixes")):
			if self {
				continue
			}
			for {
				n2, c2, self2, ok2 := s.nextTag()
				if !ok2 {
					return errTruncatedXML
				}
				if c2 { // </CommonPrefixes>
					break
				}
				if bytes.Equal(n2, []byte("Prefix")) && !self2 {
					page.CommonPrefixes = append(page.CommonPrefixes, unescape(s.leaf()))
				} else if !self2 {
					s.skipElement()
				}
			}
		case bytes.Equal(name, []byte("IsTruncated")):
			if !self {
				page.IsTruncated = bytes.Equal(s.leaf(), []byte("true"))
			}
		case bytes.Equal(name, []byte("NextContinuationToken")):
			if !self {
				page.NextToken = unescape(s.leaf())
			}
		default:
			if !self {
				s.skipElement()
			}
		}
	}
}

// parseTagging parses a GetObjectTagging response. The returned map is always
// non-nil on success — nil stays reserved for "tags not collected".
func parseTagging(body []byte) (map[string]string, error) {
	s := xmlScan{b: body}
	name, closing, self, ok := s.nextTag()
	if !ok || closing {
		return nil, errTruncatedXML
	}
	if !bytes.Equal(name, []byte("Tagging")) {
		return nil, s3BodyError(body)
	}
	tags := make(map[string]string, 4)
	if self {
		return tags, nil
	}
	for {
		name, closing, self, ok = s.nextTag()
		if !ok {
			return nil, errTruncatedXML
		}
		if closing {
			if bytes.Equal(name, []byte("Tagging")) {
				return tags, nil
			}
			continue // </TagSet>
		}
		switch {
		case bytes.Equal(name, []byte("Tag")):
			if self {
				continue
			}
			var k, v string
			for {
				n2, c2, self2, ok2 := s.nextTag()
				if !ok2 {
					return nil, errTruncatedXML
				}
				if c2 { // </Tag>
					break
				}
				switch {
				case bytes.Equal(n2, []byte("Key")):
					if !self2 {
						k = unescape(s.leaf())
					}
				case bytes.Equal(n2, []byte("Value")):
					if !self2 {
						v = unescape(s.leaf())
					}
				default:
					if !self2 {
						s.skipElement()
					}
				}
			}
			if k != "" {
				tags[k] = v
			}
		case bytes.Equal(name, []byte("TagSet")):
			// descend into it
		default:
			if !self {
				s.skipElement()
			}
		}
	}
}

// s3BodyError extracts Code/Message from an S3 <Error> body, or reports the
// body head verbatim when it isn't one.
func s3BodyError(body []byte) error {
	s := xmlScan{b: body}
	name, closing, self, ok := s.nextTag()
	if ok && !closing && !self && bytes.Equal(name, []byte("Error")) {
		var code, msg string
		for {
			n, c, self2, ok2 := s.nextTag()
			if !ok2 || (c && bytes.Equal(n, []byte("Error"))) {
				break
			}
			if c || self2 {
				continue
			}
			switch {
			case bytes.Equal(n, []byte("Code")):
				code = unescape(s.leaf())
			case bytes.Equal(n, []byte("Message")):
				msg = unescape(s.leaf())
			default:
				s.skipElement()
			}
		}
		if code != "" {
			return fmt.Errorf("s3 error %s: %s", code, msg)
		}
	}
	head := body
	if len(head) > 200 {
		head = head[:200]
	}
	return fmt.Errorf("unexpected S3 response: %q", head)
}
