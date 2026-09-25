//! HTTP response compression — Content-Encoding negotiation.
//!
//! Disabled by default. Opt-in via `app.enable_compression()`, which stores the
//! whole configuration in one atomic word. When disabled the hot path is a single
//! atomic load + branch-not-taken; zero cost over the uncompressed baseline.
//!
//! Negotiates with the client's `Accept-Encoding` (parsing q-values and
//! `identity;q=0`), applies an allowlist to content-types (skips images,
//! octet-stream, already-compressed types), and honors handler-supplied
//! `Content-Encoding` (a handler returning pre-compressed bytes is never
//! double-compressed).

use std::io::Write;

use bytes::Bytes;
use crossbeam_utils::atomic::AtomicCell;
use hyper::header::{HeaderMap, HeaderValue, CONTENT_ENCODING, CONTENT_LENGTH, VARY};
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
pub(crate) const DEFAULT_MIN_SIZE: u32 = 512;

/// The algorithms the server may use: a set, combined with `|`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Algos {
    gzip: bool,
    brotli: bool,
}

impl std::ops::BitOr for Algos {
    type Output = Algos;

    fn bitor(self, rhs: Algos) -> Algos {
        Algos {
            gzip: self.gzip || rhs.gzip,
            brotli: self.brotli || rhs.brotli,
        }
    }
}

#[cfg(test)]
const ALGO_GZIP: Algos = Algos {
    gzip: true,
    brotli: false,
};
#[cfg(test)]
const ALGO_BR: Algos = Algos {
    gzip: false,
    brotli: true,
};

/// An enabled compression configuration, levels clamped to each codec's range.
///
/// Eight bytes and 8-aligned, so `Option<Settings>` fits one native atomic word: a
/// request reads the whole configuration in one load and never sees half an update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(align(8))]
pub(crate) struct Settings {
    min_size: u32,
    algos: Algos,
    gzip_level: u8,
    brotli_quality: u8,
}

/// The process-wide configuration; `None` = disabled (the default).
static SETTINGS: AtomicCell<Option<Settings>> = AtomicCell::new(None);

const _: () = assert!(
    AtomicCell::<Option<Settings>>::is_lock_free(),
    "compression settings must fit one atomic word"
);

/// The configuration `app.configure_compression(...)` asks for; `None` when disabled.
pub(crate) fn requested(
    enabled: bool,
    min_size: u32,
    gzip: bool,
    brotli: bool,
    gzip_level: u32,
    brotli_quality: u32,
) -> Option<Settings> {
    enabled.then(|| Settings {
        min_size,
        algos: Algos { gzip, brotli },
        gzip_level: gzip_level.clamp(1, 9) as u8,
        brotli_quality: brotli_quality.clamp(0, 11) as u8,
    })
}

/// Sets the process-wide compression configuration, in one atomic store.
pub(crate) fn configure(
    enabled: bool,
    min_size: u32,
    gzip: bool,
    brotli: bool,
    gzip_level: u32,
    brotli_quality: u32,
) {
    SETTINGS.store(requested(
        enabled,
        min_size,
        gzip,
        brotli,
        gzip_level,
        brotli_quality,
    ));
}

/// The process-wide compression configuration; `None` when disabled.
#[inline]
pub(crate) fn current() -> Option<Settings> {
    SETTINGS.load()
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
fn negotiate(accept_encoding: &str, allowed: Algos) -> Option<Algo> {
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
                    if let Some(value) = strip_prefix_ignore_ascii_case(p, "q=") {
                        // Malformed q-value → 0.0 (disabled), not 1.0 (max preference).
                        q = value.trim().parse().unwrap_or(0.0);
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

    if allowed.brotli && br_q > 0.0 {
        return Some(Algo::Brotli);
    }
    if allowed.gzip && gz_q > 0.0 {
        return Some(Algo::Gzip);
    }
    None
}

/// `s` without its ASCII `prefix`, matched case-insensitively. Compares bytes, so a
/// multi-byte character across the prefix length is a mismatch, never a panic.
fn strip_prefix_ignore_ascii_case<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.as_bytes().get(..prefix.len())?;
    // The ASCII prefix matched, so `prefix.len()` is a char boundary of `s`.
    head.eq_ignore_ascii_case(prefix.as_bytes())
        .then(|| &s[prefix.len()..])
}

/// Allowlist of content-types that benefit from compression.
fn is_compressible(content_type: &str) -> bool {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    // "text/" prefix, with a subtype after it.
    if strip_prefix_ignore_ascii_case(ct, "text/").is_some_and(|subtype| !subtype.is_empty()) {
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

fn gzip_compress(data: &[u8], level: u8, out: &mut Vec<u8>) -> std::io::Result<()> {
    let mut enc = flate2::write::GzEncoder::new(out, flate2::Compression::new(level.into()));
    enc.write_all(data)?;
    enc.finish()?;
    Ok(())
}

fn brotli_compress(data: &[u8], quality: u8, out: &mut Vec<u8>) -> std::io::Result<()> {
    let params = brotli::enc::BrotliEncoderParams {
        quality: quality.into(),
        ..Default::default()
    };
    let mut reader = data;
    brotli::BrotliCompress(&mut reader, out, &params)?;
    Ok(())
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
    let settings = current()?;
    if accept_encoding.is_empty() {
        return None;
    }
    if body.len() < settings.min_size as usize {
        return None;
    }
    if !is_compressible(content_type) {
        return None;
    }

    let algo = negotiate(accept_encoding, settings.algos)?;

    let mut buf = COMPRESS_POOL
        .lock()
        .pop()
        .unwrap_or_else(|| Vec::with_capacity(body.len() / 2 + 64));
    buf.clear();

    let compressed = match algo {
        Algo::Gzip => gzip_compress(body, settings.gzip_level, &mut buf),
        Algo::Brotli => brotli_compress(body, settings.brotli_quality, &mut buf),
    };
    let shrunk = match compressed {
        Ok(()) => buf.len() < body.len(),
        Err(e) => {
            tracing::warn!(
                target: "pyronova::server",
                error = %e,
                encoding = algo.header_value(),
                "response compression failed; sending the body uncompressed"
            );
            false
        }
    };

    // Return buf to pool if compression failed or didn't shrink the body.
    if !shrunk {
        drop(PooledCompressBuf(buf));
        return None;
    }

    Some((
        Bytes::from_owner(PooledCompressBuf(buf)),
        algo.header_value(),
    ))
}

/// Mark `headers` as carrying a body compressed with `encoding`: set `Content-Encoding`,
/// drop a handler-supplied `Content-Length` (it counted the uncompressed body; hyper
/// recomputes it), and add `Accept-Encoding` to `Vary` unless a `Vary` line names it.
fn set_compression_headers(headers: &mut HeaderMap, encoding: &'static str) {
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static(encoding));
    headers.remove(CONTENT_LENGTH);
    if headers.get_all(VARY).iter().any(names_accept_encoding) {
        return;
    }
    // Merged into a lone `Vary` line; otherwise a line of its own, which a cache reads
    // the same way.
    let mut lines = headers.get_all(VARY).iter();
    let merged = match (lines.next(), lines.next()) {
        (Some(only), None) => only.to_str().ok().and_then(|v| {
            let v = v.trim_end().trim_end_matches(',').trim_end();
            HeaderValue::from_str(&format!("{v}, Accept-Encoding")).ok()
        }),
        _ => None,
    };
    if let Some(vary) = merged {
        headers.insert(VARY, vary);
    } else {
        headers.append(VARY, HeaderValue::from_static("Accept-Encoding"));
    }
}

fn names_accept_encoding(vary: &HeaderValue) -> bool {
    vary.to_str().is_ok_and(|v| {
        v.split(',')
            .any(|token| token.trim().eq_ignore_ascii_case("accept-encoding"))
    })
}

/// Maybe compress `data` in place. No-op when:
///   - globally disabled
///   - `accept_encoding` is empty or doesn't include a supported algorithm
///   - body is smaller than the configured minimum
///   - content-type is not in the compressible allowlist
///   - handler already set a `Content-Encoding` header
pub(crate) fn maybe_compress(data: &mut ResponseData, accept_encoding: &str) {
    if data.headers.contains_key("content-encoding") {
        return;
    }
    // A handler's own `content-type` header is the type that goes out.
    let content_type = data
        .headers
        .get("content-type")
        .or_else(|| data.content_type.to_str().ok());
    let Some(content_type) = content_type else {
        return;
    };
    let Some((compressed, encoding)) = try_compress(&data.body, content_type, accept_encoding)
    else {
        return;
    };
    data.body = compressed;
    set_compression_headers(data.headers.as_map_mut(), encoding);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ResponseHeaders;
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
            content_type: HeaderValue::from_static("application/json"),
            status: hyper::StatusCode::OK,
            headers: ResponseHeaders::new(),
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
            content_type: HeaderValue::from_static("application/json"),
            status: hyper::StatusCode::OK,
            headers: ResponseHeaders::new(),
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
            content_type: HeaderValue::from_static("application/json"),
            status: hyper::StatusCode::OK,
            headers: ResponseHeaders::new(),
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
            content_type: HeaderValue::from_static("image/png"),
            status: hyper::StatusCode::OK,
            headers: ResponseHeaders::new(),
        };
        maybe_compress(&mut data, "gzip, br");
        assert!(!data.headers.contains_key("content-encoding"));
        reset();
    }

    #[test]
    fn handler_content_encoding_preserved() {
        let _g = CONFIG_LOCK.lock().unwrap();
        configure(true, 100, true, true, 6, 4);
        let mut headers = ResponseHeaders::new();
        headers
            .as_map_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("identity"));
        let mut data = ResponseData {
            body: Bytes::from(vec![b'x'; 2048]),
            content_type: HeaderValue::from_static("application/json"),
            status: hyper::StatusCode::OK,
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
        let mut headers = ResponseHeaders::new();
        headers
            .as_map_mut()
            .insert(VARY, HeaderValue::from_static("Origin"));
        let payload = vec![b'a'; 4096];
        let mut data = ResponseData {
            body: Bytes::from(payload),
            content_type: HeaderValue::from_static("application/json"),
            status: hyper::StatusCode::OK,
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
            content_type: HeaderValue::from_static("application/json"),
            status: hyper::StatusCode::OK,
            headers: ResponseHeaders::new(),
        };
        maybe_compress(&mut data, "");
        assert!(!data.headers.contains_key("content-encoding"));
        reset();
    }

    #[test]
    fn is_compressible_non_ascii_does_not_panic() {
        // Byte 5 falls inside the 3-byte '€': a `str[..5]` slice panics here.
        assert!(!is_compressible("tex\u{20ac}/plain"));
        assert!(!is_compressible("\u{20ac}\u{20ac}/x"));
        assert!(is_compressible("text/\u{20ac}"));
        assert!(!is_compressible("text/"));
    }

    #[test]
    fn negotiate_non_ascii_param_does_not_panic() {
        // Byte 2 falls inside the 3-byte '€' of the parameter.
        assert_eq!(
            negotiate("br;\u{20ac}, gzip", ALGO_GZIP | ALGO_BR),
            Some(Algo::Brotli)
        );
        assert_eq!(negotiate("br;q\u{20ac}", ALGO_BR), Some(Algo::Brotli));
    }

    #[test]
    fn configuration_round_trips_as_one_value() {
        let _g = CONFIG_LOCK.lock().unwrap();
        configure(true, 100, false, true, 42, 99);
        let stored = current().expect("enabled");
        assert_eq!(stored, requested(true, 100, false, true, 42, 99).unwrap());
        assert_eq!(
            stored,
            Settings {
                min_size: 100,
                algos: ALGO_BR,
                gzip_level: 9,
                brotli_quality: 11,
            }
        );
        configure(false, 100, true, true, 6, 4);
        assert_eq!(current(), None);
        reset();
    }

    #[test]
    fn compress_helpers_report_success_as_a_result() {
        let data = vec![b'a'; 4096];
        let mut gz = Vec::new();
        gzip_compress(&data, 6, &mut gz).unwrap();
        assert!(!gz.is_empty() && gz.len() < data.len());
        let mut br = Vec::new();
        brotli_compress(&data, 4, &mut br).unwrap();
        assert!(!br.is_empty() && br.len() < data.len());
    }
}
