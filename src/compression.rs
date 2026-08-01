//! HTTP response compression — Content-Encoding negotiation.
//!
//! Disabled by default. Opt-in via `app.enable_compression()` which flips a
//! global `AtomicBool`. When disabled the hot path is a single relaxed load
//! + branch-not-taken; zero cost over the uncompressed baseline.
//!
//! Negotiates with the client's `Accept-Encoding` (parsing q-values and
//! `identity;q=0`), applies an allowlist to content-types (skips images,
//! octet-stream, already-compressed types), and honors handler-supplied
//! `Content-Encoding` (a handler returning pre-compressed bytes is never
//! double-compressed).

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::types::ResponseData;

// ---------------------------------------------------------------------------
// Per-request compression output buffer pool
// ---------------------------------------------------------------------------

// Same bounded-pool pattern as the JSON serializer's BUFFER_POOL.
// Buffers up to 4 MiB are recycled; larger ones are dropped to bound
// memory. 32 slots comfortably covers a 16-worker deployment.
static COMPRESS_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

struct PooledCompressBuf(Vec<u8>);

impl AsRef<[u8]> for PooledCompressBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for PooledCompressBuf {
    fn drop(&mut self) {
        let mut v = std::mem::take(&mut self.0);
        if v.capacity() <= 4 << 20 {
            v.clear();
            let mut pool = COMPRESS_POOL.lock();
            if pool.len() < 32 {
                pool.push(v);
            }
        }
    }
}

/// Default minimum body size to compress. Small payloads cost more CPU to
/// compress + send headers than the saved bytes.
pub(crate) const DEFAULT_MIN_SIZE: usize = 512;

static ENABLED: AtomicBool = AtomicBool::new(false);
static MIN_SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_MIN_SIZE);
/// Bit 0 = gzip allowed, bit 1 = brotli allowed.
static ALGO_MASK: AtomicUsize = AtomicUsize::new(0b11);
static GZIP_LEVEL: AtomicUsize = AtomicUsize::new(6);
static BROTLI_QUALITY: AtomicUsize = AtomicUsize::new(4);

const ALGO_GZIP: usize = 0b01;
const ALGO_BR: usize = 0b10;

pub(crate) fn configure(
    enabled: bool,
    min_size: usize,
    gzip: bool,
    brotli: bool,
    gzip_level: u32,
    brotli_quality: u32,
) {
    let mask = (if gzip { ALGO_GZIP } else { 0 }) | (if brotli { ALGO_BR } else { 0 });
    ALGO_MASK.store(mask, Ordering::Relaxed);
    MIN_SIZE.store(min_size, Ordering::Relaxed);
    GZIP_LEVEL.store(gzip_level.clamp(1, 9) as usize, Ordering::Relaxed);
    BROTLI_QUALITY.store(brotli_quality.clamp(0, 11) as usize, Ordering::Relaxed);
    ENABLED.store(enabled, Ordering::Release);
}

#[inline]
pub(crate) fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Algo {
    Gzip,
    Brotli,
}

impl Algo {
    fn header_value(self) -> &'static str {
        match self {
            Algo::Gzip => "gzip",
            Algo::Brotli => "br",
        }
    }
}

/// Parse `Accept-Encoding` and return the server-preferred algorithm the
/// client accepts. Server preference: brotli > gzip.
fn negotiate(accept_encoding: &str, mask: usize) -> Option<Algo> {
    // Track per-algo max q seen (default 1.0 if listed without q-value).
    // identity q=0 is respected for completeness but we only return Some
    // when one of our algorithms is acceptable, so it's informational.
    let mut br_q = -1.0f32;
    let mut gz_q = -1.0f32;
    let mut star_q = -1.0f32;

    for raw in accept_encoding.split(',') {
        let part = raw.trim();
        if part.is_empty() {
            continue;
        }
        let (name, q) = match part.split_once(';') {
            Some((n, rest)) => {
                let mut q = 1.0f32;
                for param in rest.split(';') {
                    let p = param.trim();
                    // Case-insensitive "q=" prefix check without heap allocation.
                    if p.len() >= 2 && p[..2].eq_ignore_ascii_case("q=") {
                        // Malformed q-value → 0.0 (disabled), not 1.0 (max preference).
                        q = p[2..].trim().parse().unwrap_or(0.0);
                        // First q= wins; duplicate params (e.g. `br;q=1.0;q=0.0`)
                        // have undefined semantics, so don't let a later write
                        // silently flip the preference. Take the first, stop.
                        break;
                    }
                }
                (n.trim(), q)
            }
            None => (part, 1.0),
        };
        // eq_ignore_ascii_case avoids a heap allocation on every token.
        if name.eq_ignore_ascii_case("br") {
            br_q = br_q.max(q);
        } else if name.eq_ignore_ascii_case("gzip") || name.eq_ignore_ascii_case("x-gzip") {
            gz_q = gz_q.max(q);
        } else if name == "*" {
            star_q = star_q.max(q);
        }
    }

    // Fill from wildcard if the specific algo wasn't mentioned.
    if br_q < 0.0 {
        br_q = star_q;
    }
    if gz_q < 0.0 {
        gz_q = star_q;
    }

    if mask & ALGO_BR != 0 && br_q > 0.0 {
        return Some(Algo::Brotli);
    }
    if mask & ALGO_GZIP != 0 && gz_q > 0.0 {
        return Some(Algo::Gzip);
    }
    None
}

/// Allowlist of content-types that benefit from compression.
fn is_compressible(content_type: &str) -> bool {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    // "text/" prefix — case-insensitive, no allocation.
    if ct.len() > 5 && ct[..5].eq_ignore_ascii_case("text/") {
        return true;
    }
    // application/* and image/svg types. The framework always emits these
    // lowercase, so exact match is sufficient; mixed-case from a handler is
    // an edge case and a miss is harmless (response stays uncompressed).
    matches!(
        ct,
        "application/json"
            | "application/javascript"
            | "application/xml"
            | "application/xhtml+xml"
            | "application/rss+xml"
            | "application/atom+xml"
            | "application/x-javascript"
            | "application/ld+json"
            | "application/manifest+json"
            | "image/svg+xml"
    )
}

fn gzip_compress(data: &[u8], level: u32, out: &mut Vec<u8>) -> bool {
    let mut enc = flate2::write::GzEncoder::new(out, flate2::Compression::new(level));
    if enc.write_all(data).is_err() {
        return false;
    }
    enc.finish().is_ok()
}

fn brotli_compress(data: &[u8], quality: u32, out: &mut Vec<u8>) -> bool {
    let params = brotli::enc::BrotliEncoderParams {
        quality: quality as i32,
        ..Default::default()
    };
    let mut reader = data;
    brotli::BrotliCompress(&mut reader, out, &params).is_ok()
}

/// Core compression primitive. Returns Some((compressed_body, encoding))
/// iff compression should be applied, else None.
///
/// Callers are responsible for swapping the body in and setting the
/// `Content-Encoding` + `Vary: Accept-Encoding` headers via
/// [`set_compression_headers`].
fn try_compress(
    body: &[u8],
    content_type: &str,
    accept_encoding: &str,
) -> Option<(Bytes, &'static str)> {
    if !is_enabled() {
        return None;
    }
    if accept_encoding.is_empty() {
        return None;
    }
    if body.len() < MIN_SIZE.load(Ordering::Relaxed) {
        return None;
    }
    if !is_compressible(content_type) {
        return None;
    }

    let mask = ALGO_MASK.load(Ordering::Relaxed);
    let algo = negotiate(accept_encoding, mask)?;

    let mut buf = COMPRESS_POOL
        .lock()
        .pop()
        .unwrap_or_else(|| Vec::with_capacity(body.len() / 2 + 64));
    buf.clear();

    let ok = match algo {
        Algo::Gzip => gzip_compress(body, GZIP_LEVEL.load(Ordering::Relaxed) as u32, &mut buf),
        Algo::Brotli => brotli_compress(
            body,
            BROTLI_QUALITY.load(Ordering::Relaxed) as u32,
            &mut buf,
        ),
    };

    // Return buf to pool if compression failed or didn't shrink the body.
    if !ok || buf.len() >= body.len() {
        drop(PooledCompressBuf(buf));
        return None;
    }

    Some((
        Bytes::from_owner(PooledCompressBuf(buf)),
        algo.header_value(),
    ))
}

/// Merge `Accept-Encoding` into an existing `Vary` header (case-insensitive)
/// and set `Content-Encoding`. Used by the main-thread `ResponseData` path.
fn set_compression_headers(
    headers: &mut std::collections::HashMap<String, String>,
    encoding: &'static str,
) {
    headers.insert("content-encoding".to_string(), encoding.to_string());
    // Remove any handler-supplied Content-Length — it reflected the pre-compression
    // size and is now wrong. hyper will recompute from the compressed body.
    headers.retain(|k, _| !k.eq_ignore_ascii_case("content-length"));
    let existing_vary = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("vary"))
        .map(|(k, v)| (k.clone(), v.clone()));
    match existing_vary {
        Some((k, v)) => {
            if !v
                .split(',')
                .any(|tok| tok.trim().eq_ignore_ascii_case("accept-encoding"))
            {
                let v_clean = v.trim_end().trim_end_matches(',').trim_end();
                headers.insert(k, format!("{v_clean}, Accept-Encoding"));
            }
        }
        None => {
            headers.insert("vary".to_string(), "Accept-Encoding".to_string());
        }
    }
}

/// Same as [`set_compression_headers`] but for the sub-interpreter path where
/// headers are stored as `Vec<(String, String)>` to support duplicate keys
/// (e.g. multiple `Set-Cookie` values).
fn set_compression_headers_vec(headers: &mut Vec<(String, String)>, encoding: &'static str) {
    headers.push(("content-encoding".to_string(), encoding.to_string()));
    // Remove any handler-supplied Content-Length — it reflected the pre-compression
    // size and is now wrong. hyper will recompute from the compressed body.
    headers.retain(|(k, _)| !k.eq_ignore_ascii_case("content-length"));
    let existing_vary = headers
        .iter()
        .position(|(k, _)| k.eq_ignore_ascii_case("vary"));
    match existing_vary {
        Some(idx) => {
            let v = &headers[idx].1;
            if !v
                .split(',')
                .any(|tok| tok.trim().eq_ignore_ascii_case("accept-encoding"))
            {
                let v_clean = v.trim_end().trim_end_matches(',').trim_end();
                let new_val = format!("{v_clean}, Accept-Encoding");
                headers[idx].1 = new_val;
            }
        }
        None => {
            headers.push(("vary".to_string(), "Accept-Encoding".to_string()));
        }
    }
}

/// Maybe compress `data` in place. No-op when:
///   - globally disabled
///   - `accept_encoding` is empty or doesn't include a supported algorithm
///   - body is smaller than the configured minimum
///   - content-type is not in the compressible allowlist
///   - handler already set a `Content-Encoding` header
pub(crate) fn maybe_compress(data: &mut ResponseData, accept_encoding: &str) {
    if data
        .headers
        .keys()
        .any(|k| k.eq_ignore_ascii_case("content-encoding"))
    {
        return;
    }
    let Some((compressed, encoding)) =
        try_compress(&data.body, &data.content_type, accept_encoding)
    else {
        return;
    };
    data.body = compressed;
    set_compression_headers(&mut data.headers, encoding);
}

/// Variant used by the sub-interpreter fast path.
///
/// Takes `body` by value to avoid the `Bytes → Vec` copy that the old
/// `&mut Vec<u8>` + `compressed.to_vec()` pattern required. Returns
/// `Bytes` directly: the compressed pool buffer or (if no compression)
/// the original `Vec<u8>` wrapped in `Bytes::from` (O(1) ownership
/// transfer, no copy).
pub(crate) fn maybe_compress_subinterp(
    body: Vec<u8>,
    content_type: &str,
    headers: &mut Vec<(String, String)>,
    accept_encoding: &str,
) -> Bytes {
    if headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-encoding"))
    {
        return Bytes::from(body);
    }
    let Some((compressed, encoding)) = try_compress(&body, content_type, accept_encoding) else {
        return Bytes::from(body);
    };
    set_compression_headers_vec(headers, encoding);
    compressed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // Tests share global config state — serialize them to prevent races.
    static CONFIG_LOCK: Mutex<()> = Mutex::new(());

    fn reset() {
        configure(false, DEFAULT_MIN_SIZE, true, true, 6, 4);
    }

    #[test]
    fn negotiate_picks_brotli_over_gzip() {
        let algo = negotiate("gzip, deflate, br", ALGO_GZIP | ALGO_BR);
        assert_eq!(algo, Some(Algo::Brotli));
    }

    #[test]
    fn negotiate_gzip_only_when_brotli_disabled() {
        let algo = negotiate("gzip, br", ALGO_GZIP);
        assert_eq!(algo, Some(Algo::Gzip));
    }

    #[test]
    fn negotiate_respects_q_zero() {
        let algo = negotiate("br;q=0, gzip", ALGO_GZIP | ALGO_BR);
        assert_eq!(algo, Some(Algo::Gzip));
    }

    #[test]
    fn negotiate_wildcard() {
        let algo = negotiate("*", ALGO_GZIP | ALGO_BR);
        assert_eq!(algo, Some(Algo::Brotli));
    }

    #[test]
    fn negotiate_wildcard_q_zero_excludes() {
        // "*;q=0, gzip" — explicitly listed gzip wins, brotli excluded by wildcard
        let algo = negotiate("*;q=0, gzip", ALGO_GZIP | ALGO_BR);
        assert_eq!(algo, Some(Algo::Gzip));
    }

    #[test]
    fn negotiate_none_when_unsupported() {
        assert_eq!(negotiate("deflate, compress", ALGO_GZIP | ALGO_BR), None);
    }

    #[test]
    fn negotiate_uppercase_q_param() {
        // Q= should be treated the same as q= (case-insensitive param name).
        let algo = negotiate("br;Q=0, gzip", ALGO_GZIP | ALGO_BR);
        assert_eq!(algo, Some(Algo::Gzip));
    }

    #[test]
    fn negotiate_uppercase_algo_names() {
        // BR / GZIP in caps — real clients don't do this but we should be robust.
        let algo = negotiate("BR, GZIP", ALGO_GZIP | ALGO_BR);
        assert_eq!(algo, Some(Algo::Brotli));
    }

    #[test]
    fn negotiate_malformed_q_disables() {
        // Malformed q-value should disable the algorithm (0.0), not enable it (1.0).
        let algo = negotiate("br;q=bad, gzip", ALGO_GZIP | ALGO_BR);
        assert_eq!(algo, Some(Algo::Gzip));
    }

    #[test]
    fn is_compressible_mixed_case_text() {
        assert!(is_compressible("Text/HTML"));
        assert!(is_compressible("TEXT/plain; charset=utf-8"));
    }

    #[test]
    fn is_compressible_exact_application_types() {
        assert!(is_compressible("application/json"));
        assert!(is_compressible("application/ld+json"));
        assert!(!is_compressible("application/octet-stream"));
    }

    #[test]
    fn compressible_allowlist() {
        assert!(is_compressible("text/html"));
        assert!(is_compressible("text/html; charset=utf-8"));
        assert!(is_compressible("application/json"));
        assert!(is_compressible("image/svg+xml"));
        assert!(!is_compressible("image/png"));
        assert!(!is_compressible("application/octet-stream"));
        assert!(!is_compressible("video/mp4"));
    }

    #[test]
    fn disabled_by_default_is_noop() {
        let _g = CONFIG_LOCK.lock().unwrap();
        reset();
        let mut data = ResponseData {
            body: Bytes::from(vec![b'x'; 2048]),
            content_type: "application/json".to_string(),
            status: 200,
            headers: HashMap::new(),
        };
        let before = data.body.clone();
        maybe_compress(&mut data, "gzip, br");
        assert_eq!(data.body, before);
        assert!(!data.headers.contains_key("content-encoding"));
    }

    #[test]
    fn enabled_compresses_json() {
        let _g = CONFIG_LOCK.lock().unwrap();
        configure(true, 100, true, true, 6, 4);
        let payload = serde_json::to_vec(&serde_json::json!({
            "items": vec!["hello world"; 100]
        }))
        .unwrap();
        let mut data = ResponseData {
            body: Bytes::from(payload.clone()),
            content_type: "application/json".to_string(),
            status: 200,
            headers: HashMap::new(),
        };
        maybe_compress(&mut data, "br, gzip");
        assert!(data.body.len() < payload.len());
        assert_eq!(data.headers.get("content-encoding").unwrap(), "br");
        assert_eq!(
            data.headers.get("vary").unwrap().to_ascii_lowercase(),
            "accept-encoding"
        );
        reset();
    }

    #[test]
    fn small_body_skipped() {
        let _g = CONFIG_LOCK.lock().unwrap();
        configure(true, 512, true, true, 6, 4);
        let mut data = ResponseData {
            body: Bytes::from("small"),
            content_type: "application/json".to_string(),
            status: 200,
            headers: HashMap::new(),
        };
        maybe_compress(&mut data, "gzip, br");
        assert!(!data.headers.contains_key("content-encoding"));
        reset();
    }

    #[test]
    fn binary_content_type_skipped() {
        let _g = CONFIG_LOCK.lock().unwrap();
        configure(true, 100, true, true, 6, 4);
        let mut data = ResponseData {
            body: Bytes::from(vec![0u8; 2048]),
            content_type: "image/png".to_string(),
            status: 200,
            headers: HashMap::new(),
        };
        maybe_compress(&mut data, "gzip, br");
        assert!(!data.headers.contains_key("content-encoding"));
        reset();
    }

    #[test]
    fn handler_content_encoding_preserved() {
        let _g = CONFIG_LOCK.lock().unwrap();
        configure(true, 100, true, true, 6, 4);
        let mut headers = HashMap::new();
        headers.insert("Content-Encoding".to_string(), "identity".to_string());
        let mut data = ResponseData {
            body: Bytes::from(vec![b'x'; 2048]),
            content_type: "application/json".to_string(),
            status: 200,
            headers,
        };
        maybe_compress(&mut data, "gzip, br");
        // No override; existing header stays.
        assert_eq!(data.headers.get("Content-Encoding").unwrap(), "identity");
        reset();
    }

    #[test]
    fn vary_merges_with_existing() {
        let _g = CONFIG_LOCK.lock().unwrap();
        configure(true, 100, true, true, 6, 4);
        let mut headers = HashMap::new();
        headers.insert("Vary".to_string(), "Origin".to_string());
        let payload = vec![b'a'; 4096];
        let mut data = ResponseData {
            body: Bytes::from(payload),
            content_type: "application/json".to_string(),
            status: 200,
            headers,
        };
        maybe_compress(&mut data, "gzip");
        let vary = data.headers.get("Vary").unwrap();
        assert!(vary.to_ascii_lowercase().contains("origin"));
        assert!(vary.to_ascii_lowercase().contains("accept-encoding"));
        reset();
    }

    #[test]
    fn empty_accept_encoding_noop() {
        let _g = CONFIG_LOCK.lock().unwrap();
        configure(true, 100, true, true, 6, 4);
        let mut data = ResponseData {
            body: Bytes::from(vec![b'x'; 2048]),
            content_type: "application/json".to_string(),
            status: 200,
            headers: HashMap::new(),
        };
        maybe_compress(&mut data, "");
        assert!(!data.headers.contains_key("content-encoding"));
        reset();
    }
}
