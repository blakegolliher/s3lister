// rust-fastlist: a spike measuring what a Rust port of s3lister's hot path
// buys over the Go implementation. Three benchmarks:
//
//   bench-parse            CPU-only: the fastxml ListObjectsV2 parser ported
//                          to Rust, on the same synthetic 1000-key page the
//                          Go benchmark uses. Borrowed mode parses zero-copy
//                          (the structural advantage Go cannot have); owned
//                          mode allocates key/etag Strings like Go must.
//   bench-list             Wire: flat-out ListObjectsV2 pagination over the
//                          bench layout's data/dNNN/sNNN/ prefixes.
//   bench-tags             Wire: flat-out GetObjectTagging over the bench
//                          layout's deterministic keys.
//
// The Go counterpart for the wire modes is spike/go-fastlist, which reuses
// s3lister's FastClient so both sides are lean clients doing identical work.
// http:// endpoints only (the lab setup); no TLS stack is compiled in.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------- XML scan

enum Tag<'a> {
    Open(&'a [u8], bool), // name, self-closing
    Close(&'a [u8]),
}

struct Scan<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Scan<'a> {
    fn new(b: &'a [u8]) -> Self {
        Scan { b, p: 0 }
    }

    fn next_tag(&mut self) -> Option<Tag<'a>> {
        loop {
            let i = memchr::memchr(b'<', &self.b[self.p..])?;
            self.p += i + 1;
            let c = *self.b.get(self.p)?;
            if c == b'?' || c == b'!' {
                let j = memchr::memchr(b'>', &self.b[self.p..])?;
                self.p += j + 1;
                continue;
            }
            let closing = c == b'/';
            if closing {
                self.p += 1;
            }
            let start = self.p;
            while self.p < self.b.len() {
                match self.b[self.p] {
                    b' ' | b'\t' | b'\n' | b'\r' | b'>' | b'/' => break,
                    _ => self.p += 1,
                }
            }
            let name = &self.b[start..self.p];
            let j = memchr::memchr(b'>', &self.b[self.p..])?;
            let self_close = !closing && self.p + j > start && self.b[self.p + j - 1] == b'/';
            self.p += j + 1;
            return Some(if closing {
                Tag::Close(name)
            } else {
                Tag::Open(name, self_close)
            });
        }
    }

    fn leaf(&mut self) -> &'a [u8] {
        match memchr::memchr(b'<', &self.b[self.p..]) {
            None => {
                let t = &self.b[self.p..];
                self.p = self.b.len();
                t
            }
            Some(i) => {
                let t = &self.b[self.p..self.p + i];
                self.p += i;
                self.next_tag();
                t
            }
        }
    }

    fn skip_element(&mut self) {
        let mut depth = 1u32;
        while depth > 0 {
            match self.next_tag() {
                None => return,
                Some(Tag::Open(_, false)) => depth += 1,
                Some(Tag::Open(_, true)) => {}
                Some(Tag::Close(_)) => depth -= 1,
            }
        }
    }
}

// ------------------------------------------------------------ list parsing

#[allow(dead_code)]
struct ObjRef<'a> {
    key: &'a [u8],
    etag: &'a [u8],
    storage: &'a [u8],
    size: i64,
    lm_nanos: i64,
}

/// Parses a ListObjectsV2 response, invoking f per object. Returns
/// (is_truncated, next_token). Zero-copy: every field borrows the body.
fn parse_list<'a, F: FnMut(ObjRef<'a>)>(
    body: &'a [u8],
    mut f: F,
) -> Result<(bool, Option<String>), String> {
    let mut s = Scan::new(body);
    match s.next_tag() {
        Some(Tag::Open(name, false)) if name == b"ListBucketResult" => {}
        Some(Tag::Open(name, true)) if name == b"ListBucketResult" => return Ok((false, None)),
        _ => return Err(format!("unexpected response: {:?}", head(body))),
    }
    let mut truncated = false;
    let mut token: Option<String> = None;
    loop {
        match s.next_tag() {
            None => return Err("truncated XML".into()),
            Some(Tag::Close(_)) => return Ok((truncated, token)),
            Some(Tag::Open(name, self_close)) => match name {
                b"Contents" if !self_close => {
                    let mut o = ObjRef {
                        key: b"",
                        etag: b"",
                        storage: b"",
                        size: 0,
                        lm_nanos: 0,
                    };
                    loop {
                        match s.next_tag() {
                            None => return Err("truncated XML".into()),
                            Some(Tag::Close(_)) => break,
                            Some(Tag::Open(n, sc)) => match n {
                                b"Key" if !sc => o.key = s.leaf(),
                                b"LastModified" if !sc => o.lm_nanos = parse_s3_time(s.leaf())?,
                                b"ETag" if !sc => o.etag = s.leaf(),
                                b"Size" if !sc => o.size = atoi(s.leaf()),
                                b"StorageClass" if !sc => o.storage = s.leaf(),
                                _ if !sc => s.skip_element(),
                                _ => {}
                            },
                        }
                    }
                    if !o.key.is_empty() {
                        f(o);
                    }
                }
                b"IsTruncated" if !self_close => truncated = s.leaf() == b"true",
                b"NextContinuationToken" if !self_close => {
                    token = Some(String::from_utf8_lossy(s.leaf()).into_owned())
                }
                _ if !self_close => s.skip_element(),
                _ => {}
            },
        }
    }
}

/// Parses a GetObjectTagging response, returning the number of tags.
fn parse_tagging(body: &[u8]) -> Result<usize, String> {
    let mut s = Scan::new(body);
    match s.next_tag() {
        Some(Tag::Open(name, false)) if name == b"Tagging" => {}
        Some(Tag::Open(name, true)) if name == b"Tagging" => return Ok(0),
        _ => return Err(format!("unexpected response: {:?}", head(body))),
    }
    let mut n = 0usize;
    loop {
        match s.next_tag() {
            None => return Err("truncated XML".into()),
            Some(Tag::Close(name)) if name == b"Tagging" => return Ok(n),
            Some(Tag::Close(_)) => {}
            Some(Tag::Open(name, self_close)) => match name {
                b"Tag" if !self_close => {
                    loop {
                        match s.next_tag() {
                            None => return Err("truncated XML".into()),
                            Some(Tag::Close(_)) => break,
                            Some(Tag::Open(n2, sc)) => match n2 {
                                b"Key" | b"Value" if !sc => {
                                    s.leaf();
                                }
                                _ if !sc => s.skip_element(),
                                _ => {}
                            },
                        }
                    }
                    n += 1;
                }
                b"TagSet" => {}
                _ if !self_close => s.skip_element(),
                _ => {}
            },
        }
    }
}

fn head(b: &[u8]) -> String {
    String::from_utf8_lossy(&b[..b.len().min(120)]).into_owned()
}

fn atoi(b: &[u8]) -> i64 {
    let mut n = 0i64;
    for &c in b {
        if !c.is_ascii_digit() {
            return n;
        }
        n = n * 10 + (c - b'0') as i64;
    }
    n
}

/// Fixed-layout parse of 2026-07-21T20:15:42.123Z into epoch nanoseconds.
fn parse_s3_time(b: &[u8]) -> Result<i64, String> {
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return Err(format!("bad timestamp {:?}", head(b)));
    }
    let num = |r: std::ops::Range<usize>| -> i64 { atoi(&b[r]) };
    let (y, mo, d) = (num(0..4), num(5..7), num(8..10));
    let (h, mi, sec) = (num(11..13), num(14..16), num(17..19));
    let mut nanos = 0i64;
    let mut p = 19;
    if b[p] == b'.' {
        p += 1;
        let mut mult = 100_000_000i64;
        while p < b.len() && b[p].is_ascii_digit() {
            nanos += (b[p] - b'0') as i64 * mult;
            mult /= 10;
            p += 1;
        }
    }
    if p != b.len() - 1 || b[p] != b'Z' {
        return Err(format!("bad timestamp {:?}", head(b)));
    }
    let days = days_from_civil(y, mo, d);
    Ok(((days * 86400 + h * 3600 + mi * 60 + sec) * 1_000_000_000) + nanos)
}

/// Howard Hinnant's civil-date algorithm: days since 1970-01-01.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ------------------------------------------------------------------ SigV4

const EMPTY_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn hex256(d: &[u8]) -> String {
    let h = Sha256::digest(d);
    to_hex(&h)
}

fn to_hex(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for &c in b {
        s.push(HEX[(c >> 4) as usize] as char);
        s.push(HEX[(c & 0xf) as usize] as char);
    }
    s
}

fn hmac256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(key).unwrap();
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

fn amz_dates() -> (String, String) {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let (y, mo, d) = civil_from_days(secs.div_euclid(86400));
    let rem = secs.rem_euclid(86400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    (
        format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z"),
        format!("{y:04}{mo:02}{d:02}"),
    )
}

struct Signer {
    access: String,
    secret: String,
    region: String,
    host: String, // host[:port] exactly as it appears in the URL
}

impl Signer {
    /// Signs a body-less GET for path (already escaped) and query (already in
    /// canonical sorted+escaped form). Returns headers to attach.
    fn sign(&self, path: &str, query: &str) -> [(&'static str, String); 3] {
        let (amz_date, date) = amz_dates();
        let canonical = format!(
            "GET\n{path}\n{query}\nhost:{}\nx-amz-content-sha256:{EMPTY_SHA}\nx-amz-date:{amz_date}\n\nhost;x-amz-content-sha256;x-amz-date\n{EMPTY_SHA}",
            self.host
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let sts = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex256(canonical.as_bytes())
        );
        let mut k = hmac256(format!("AWS4{}", self.secret).as_bytes(), date.as_bytes());
        for part in [self.region.as_bytes(), b"s3", b"aws4_request"] {
            k = hmac256(&k, part);
        }
        let sig = to_hex(&hmac256(&k, sts.as_bytes()));
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}",
            self.access
        );
        [
            ("authorization", auth),
            ("x-amz-date", amz_date),
            ("x-amz-content-sha256", EMPTY_SHA.to_string()),
        ]
    }
}

fn escape(s: &str, keep_slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &c in s.as_bytes() {
        match c {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(c as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => {
                out.push('%');
                out.push(HEX[(c >> 4) as usize] as char);
                out.push(HEX[(c & 0xf) as usize] as char);
            }
        }
    }
    out
}

// ------------------------------------------------------------- bench-parse

fn synthetic_page() -> Vec<u8> {
    let mut s = String::with_capacity(100_000);
    s.push_str(r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bench-2b</Name><Prefix>data/d000/s000/</Prefix><KeyCount>1000</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>true</IsTruncated><NextContinuationToken>data/d000/s000/obj-000000000999.log</NextContinuationToken>"#);
    for i in 0..1000 {
        s.push_str(&format!(
            "<Contents><Key>data/d000/s000/obj-{i:012}.log</Key><LastModified>2026-07-21T20:15:42.123Z</LastModified><ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>0</Size><StorageClass>STANDARD</StorageClass></Contents>"
        ));
    }
    s.push_str("</ListBucketResult>");
    s.into_bytes()
}

fn bench_parse() {
    let page = synthetic_page();
    let mb = page.len() as f64 / 1e6;

    // Borrowed: zero-copy — every field is a slice of the response buffer.
    let iters = 5000;
    let mut sink = 0u64;
    let start = Instant::now();
    for _ in 0..iters {
        let mut count = 0u64;
        parse_list(&page, |o| {
            count += o.key.len() as u64 + o.size as u64 + (o.lm_nanos != 0) as u64;
        })
        .unwrap();
        sink = sink.wrapping_add(count);
    }
    let e = start.elapsed();
    let per = e.as_nanos() as f64 / iters as f64;
    println!(
        "parse borrowed : {:>10.0} ns/page  {:>7.1} MB/s  ({} iters, sink {})",
        per,
        mb / (per / 1e9),
        iters,
        std::hint::black_box(sink)
    );

    // Owned: allocate key+etag Strings per object, as Go must.
    let mut keys: Vec<String> = Vec::with_capacity(1000);
    let mut etags: Vec<String> = Vec::with_capacity(1000);
    let start = Instant::now();
    for _ in 0..iters {
        keys.clear();
        etags.clear();
        parse_list(&page, |o| {
            keys.push(String::from_utf8_lossy(o.key).into_owned());
            etags.push(String::from_utf8_lossy(o.etag).into_owned());
        })
        .unwrap();
    }
    let e = start.elapsed();
    let per = e.as_nanos() as f64 / iters as f64;
    println!(
        "parse owned    : {:>10.0} ns/page  {:>7.1} MB/s  ({} iters, {} keys/page)",
        per,
        mb / (per / 1e9),
        iters,
        std::hint::black_box(keys.len())
    );
}

// -------------------------------------------------------------- wire modes

struct Wire {
    clients: Vec<reqwest::Client>,
    next_client: AtomicUsize,
    signer: Signer,
    base: String, // http://host[:port]
    bucket: String,
}

impl Wire {
    fn client(&self) -> &reqwest::Client {
        &self.clients[self.next_client.fetch_add(1, Ordering::Relaxed) % self.clients.len()]
    }

    async fn get(&self, path: &str, query: &str) -> Result<Vec<u8>, String> {
        let url = format!("{}{}?{}", self.base, path, query);
        let mut req = self.client().get(&url);
        for (k, v) in self.signer.sign(path, query) {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status();
        let body = resp.bytes().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status, head(&body)));
        }
        Ok(body.to_vec())
    }
}

async fn build_wire(args: &Args) -> Arc<Wire> {
    let url = args.endpoint.trim_end_matches('/');
    let hostport = url
        .strip_prefix("http://")
        .expect("spike supports http:// endpoints only");
    let lookup = if hostport.contains(':') {
        hostport.to_string()
    } else {
        format!("{hostport}:80")
    };
    let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(&lookup)
        .await
        .expect("resolve endpoint")
        .collect();
    println!("endpoint {hostport} resolves to {} address(es)", addrs.len());
    let host = hostport.split(':').next().unwrap().to_string();
    // One client pinned per resolved IP, round-robined per request — the
    // same VIP spreading s3lister's dialer does.
    let clients = addrs
        .iter()
        .map(|a| {
            reqwest::Client::builder()
                .resolve(&host, *a)
                .pool_max_idle_per_host(4096)
                .timeout(Duration::from_secs(120))
                .build()
                .unwrap()
        })
        .collect();
    Arc::new(Wire {
        clients,
        next_client: AtomicUsize::new(0),
        signer: Signer {
            access: args.access.clone(),
            secret: args.secret.clone(),
            region: args.region.clone(),
            host: hostport.to_string(),
        },
        base: url.to_string(),
        bucket: args.bucket.clone(),
    })
}

const EXTS: [&str; 10] = [
    "log", "jpg", "json", "parquet", "csv", "bin", "txt", "gz", "png", "pdf",
];

/// The bench layout's deterministic index->key mapping (defaults).
fn bench_key(i: i64) -> String {
    if i % 100 < 10 {
        format!("flat/f{:02}/o-{:012}", (i / 100) % 16, i)
    } else {
        format!(
            "data/d{:03}/s{:03}/obj-{:012}.{}",
            i % 64,
            (i / 64) % 64,
            i,
            EXTS[(i % 10) as usize]
        )
    }
}

async fn bench_list(args: Args) {
    let wire = build_wire(&args).await;
    let counter = Arc::new(AtomicI64::new(0));
    let objects = Arc::new(AtomicI64::new(0));
    let errors = Arc::new(AtomicI64::new(0));
    let deadline = Instant::now() + Duration::from_secs(args.seconds);

    let mut tasks = Vec::new();
    for _ in 0..args.workers {
        let (wire, counter, objects, errors) =
            (wire.clone(), counter.clone(), objects.clone(), errors.clone());
        tasks.push(tokio::spawn(async move {
            let path = format!("/{}", wire.bucket);
            while Instant::now() < deadline {
                let idx = counter.fetch_add(1, Ordering::Relaxed) % 4096;
                let prefix = format!("data/d{:03}/s{:03}/", idx / 64, idx % 64);
                let mut token: Option<String> = None;
                loop {
                    let mut query = String::new();
                    if let Some(t) = &token {
                        query.push_str("continuation-token=");
                        query.push_str(&escape(t, false));
                        query.push('&');
                    }
                    query.push_str("list-type=2&max-keys=1000&prefix=");
                    query.push_str(&escape(&prefix, false));
                    match wire.get(&path, &query).await {
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Ok(body) => {
                            let mut n = 0i64;
                            match parse_list(&body, |_| n += 1) {
                                Err(_) => {
                                    errors.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Ok((truncated, next)) => {
                                    objects.fetch_add(n, Ordering::Relaxed);
                                    if !truncated {
                                        break;
                                    }
                                    token = next;
                                }
                            }
                        }
                    }
                    if Instant::now() >= deadline {
                        break;
                    }
                }
            }
        }));
    }
    report(objects.clone(), errors.clone(), deadline, "objs").await;
    for t in tasks {
        let _ = t.await;
    }
}

async fn bench_tags(args: Args) {
    let wire = build_wire(&args).await;
    let counter = Arc::new(AtomicI64::new(0));
    let fetched = Arc::new(AtomicI64::new(0));
    let errors = Arc::new(AtomicI64::new(0));
    let count = args.count;
    let deadline = Instant::now() + Duration::from_secs(args.seconds);

    let mut tasks = Vec::new();
    for _ in 0..args.workers {
        let (wire, counter, fetched, errors) =
            (wire.clone(), counter.clone(), fetched.clone(), errors.clone());
        tasks.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                let i = counter.fetch_add(1, Ordering::Relaxed) % count;
                let key = bench_key(i);
                let path = format!("/{}/{}", wire.bucket, escape(&key, true));
                match wire.get(&path, "tagging=").await {
                    Ok(body) => match parse_tagging(&body) {
                        Ok(_) => {
                            fetched.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    report(fetched.clone(), errors.clone(), deadline, "tags").await;
    for t in tasks {
        let _ = t.await;
    }
}

async fn report(count: Arc<AtomicI64>, errors: Arc<AtomicI64>, deadline: Instant, unit: &str) {
    let start = Instant::now();
    let mut last = 0i64;
    let mut last_t = start;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(5).min(deadline - Instant::now())).await;
        let now = Instant::now();
        let c = count.load(Ordering::Relaxed);
        let rate = (c - last) as f64 / (now - last_t).as_secs_f64();
        println!(
            "{:>12} {unit}  {:>9.0}/s  errors={}",
            c,
            rate,
            errors.load(Ordering::Relaxed)
        );
        last = c;
        last_t = now;
    }
    let total = count.load(Ordering::Relaxed);
    let secs = start.elapsed().as_secs_f64();
    println!(
        "TOTAL: {} {unit} in {:.1}s = {:.0}/s  errors={}",
        total,
        secs,
        total as f64 / secs,
        errors.load(Ordering::Relaxed)
    );
}

// -------------------------------------------------------------------- main

struct Args {
    endpoint: String,
    access: String,
    secret: String,
    region: String,
    bucket: String,
    workers: usize,
    seconds: u64,
    count: i64,
}

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() - 1 {
        if args[i] == format!("--{name}") {
            return args[i + 1].clone();
        }
    }
    default.to_string()
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    if mode == "bench-parse" {
        bench_parse();
        return;
    }
    let args = Args {
        endpoint: arg("endpoint", ""),
        access: arg("access-key", ""),
        secret: arg("secret-key", ""),
        region: arg("region", "us-east-1"),
        bucket: arg("bucket", ""),
        workers: arg("workers", "256").parse().unwrap(),
        seconds: arg("seconds", "60").parse().unwrap(),
        count: arg("count", "100000000").parse().unwrap(),
    };
    match mode.as_str() {
        "bench-list" | "bench-tags" => {
            if args.endpoint.is_empty() || args.bucket.is_empty() || args.access.is_empty() {
                eprintln!("required: --endpoint http://host --bucket B --access-key K --secret-key S");
                std::process::exit(1);
            }
            let rt = tokio::runtime::Runtime::new().unwrap();
            if mode == "bench-list" {
                rt.block_on(bench_list(args));
            } else {
                rt.block_on(bench_tags(args));
            }
        }
        _ => {
            eprintln!("usage: rust-fastlist <bench-parse|bench-list|bench-tags> [--flags]");
            eprintln!("  bench-list/tags: --endpoint http://host --bucket B --access-key K --secret-key S");
            eprintln!("                   [--workers 256] [--seconds 60] [--count 100000000] [--region us-east-1]");
            std::process::exit(1);
        }
    }
}
