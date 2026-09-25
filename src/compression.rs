//! HTTP response compression — Content-Encoding negotiation.
//!
//! Disabled by default. Each app opts in with `app.enable_compression()`, which hands the
//! engine a validated [`Settings`]; a run serves it from its `SiteConfig`. When disabled
//! the hot path is one `Option` check.
//!
//! Negotiates with the client's `Accept-Encoding` (parsing q-values and
//! `identity;q=0`), applies an allowlist to content-types (skips images,
//! octet-stream, already-compressed types), and honors handler-supplied
//! `Content-Encoding` (a handler returning pre-compressed bytes is never
//! double-compressed). A large body, or one for a slow codec setting, is compressed on
//! the blocking pool so it never stalls a tokio worker.

use std::io::Write;

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderValue, CONTENT_ENCODING, CONTENT_LENGTH, VARY};
use parking_lot::Mutex;
use pyo3::prelude::*;

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

// ---------------------------------------------------------------------------
// Settings, validated once at the Python edge
// ---------------------------------------------------------------------------

/// Default minimum body size to compress. Small payloads cost more CPU to
/// compress + send headers than the saved bytes.
const DEFAULT_MIN_SIZE: u32 = 512;
const DEFAULT_GZIP_LEVEL: i64 = 6;
const DEFAULT_BROTLI_QUALITY: i64 = 4;

/// The algorithms the server may use; never none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Algos {
    Gzip,
    Brotli,
    Both,
}

impl Algos {
    fn of(gzip: bool, brotli: bool) -> Result<Self, SettingsError> {
        match (gzip, brotli) {
            (true, true) => Ok(Self::Both),
            (true, false) => Ok(Self::Gzip),
            (false, true) => Ok(Self::Brotli),
            (false, false) => Err(SettingsError::NoAlgorithm),
        }
    }

    fn gzip(self) -> bool {
        matches!(self, Self::Gzip | Self::Both)
    }

    fn brotli(self) -> bool {
        matches!(self, Self::Brotli | Self::Both)
    }
}

/// A gzip compression level, 1..=9.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GzipLevel(u8);

impl GzipLevel {
    fn new(level: i64) -> Result<Self, SettingsError> {
        match u8::try_from(level) {
            Ok(level @ 1..=9) => Ok(Self(level)),
            _ => Err(SettingsError::GzipLevel(level)),
        }
    }
}

/// A brotli quality, 0..=11.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BrotliQuality(u8);

impl BrotliQuality {
    /// At or above this quality brotli is slow enough (~1 MB/s at 11) that even a small
    /// body is compressed off the tokio worker.
    const SLOW: u8 = 10;

    fn new(quality: i64) -> Result<Self, SettingsError> {
        match u8::try_from(quality) {
            Ok(quality @ 0..=11) => Ok(Self(quality)),
            _ => Err(SettingsError::BrotliQuality(quality)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
enum SettingsError {
    #[error("min_size must be 0..={max} bytes, got {0}", max = u32::MAX)]
    MinSize(i64),
    #[error("gzip_level must be 1..=9, got {0}")]
    GzipLevel(i64),
    #[error("brotli_quality must be 0..=11, got {0}")]
    BrotliQuality(i64),
    #[error(
        "gzip=False and brotli=False enables no algorithm; use disable_compression() to turn \
         compression off"
    )]
    NoAlgorithm,
}

impl From<SettingsError> for PyErr {
    fn from(e: SettingsError) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}

/// An app's compression settings: `Compression(min_size=512, gzip=True, brotli=True,
/// gzip_level=6, brotli_quality=4)`. A level out of its codec's range, or neither
/// algorithm, is a `ValueError` here, never clamped or ignored later.
#[pyclass(frozen, name = "Compression", module = "pyronova.engine")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Settings {
    min_size: u32,
    algos: Algos,
    gzip_level: GzipLevel,
    brotli_quality: BrotliQuality,
}

impl Settings {
    fn parse(
        min_size: i64,
        gzip: bool,
        brotli: bool,
        gzip_level: i64,
        brotli_quality: i64,
    ) -> Result<Self, SettingsError> {
        Ok(Self {
            min_size: u32::try_from(min_size).map_err(|_| SettingsError::MinSize(min_size))?,
            algos: Algos::of(gzip, brotli)?,
            gzip_level: GzipLevel::new(gzip_level)?,
            brotli_quality: BrotliQuality::new(brotli_quality)?,
        })
    }
}

#[pymethods]
impl Settings {
    #[new]
    #[pyo3(signature = (
        *,
        min_size = i64::from(DEFAULT_MIN_SIZE),
        gzip = true,
        brotli = true,
        gzip_level = DEFAULT_GZIP_LEVEL,
        brotli_quality = DEFAULT_BROTLI_QUALITY,
    ))]
    fn py_new(
        min_size: i64,
        gzip: bool,
        brotli: bool,
        gzip_level: i64,
        brotli_quality: i64,
    ) -> PyResult<Self> {
        Ok(Self::parse(
            min_size,
            gzip,
            brotli,
            gzip_level,
            brotli_quality,
        )?)
    }

    #[getter]
    fn min_size(&self) -> u32 {
        self.min_size
    }

    #[getter]
    fn gzip(&self) -> bool {
        self.algos.gzip()
    }

    #[getter]
    fn brotli(&self) -> bool {
        self.algos.brotli()
    }

    #[getter]
    fn gzip_level(&self) -> u8 {
        self.gzip_level.0
    }

    #[getter]
    fn brotli_quality(&self) -> u8 {
        self.brotli_quality.0
    }

    fn __repr__(&self) -> String {
        format!(
            "Compression(min_size={}, gzip={}, brotli={}, gzip_level={}, brotli_quality={})",
            self.min_size,
            py_bool(self.algos.gzip()),
            py_bool(self.algos.brotli()),
            self.gzip_level.0,
            self.brotli_quality.0
        )
    }
}

fn py_bool(b: bool) -> &'static str {
    if b {
        "True"
    } else {
        "False"
    }
}

/// A codec with the level it runs at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Algo {
    Gzip(GzipLevel),
    Brotli(BrotliQuality),
}

impl Algo {
    fn header_value(self) -> &'static str {
        match self {
            Algo::Gzip(_) => "gzip",
            Algo::Brotli(_) => "br",
        }
    }
}

/// The codec [`negotiate`] picked, at this app's level for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Coding {
    Gzip,
    Brotli,
}

impl Coding {
    fn at(self, settings: &Settings) -> Algo {
        match self {
            Coding::Gzip => Algo::Gzip(settings.gzip_level),
            Coding::Brotli => Algo::Brotli(settings.brotli_quality),
        }
    }
}

/// Parse `Accept-Encoding` and return the server-preferred algorithm the
/// client accepts. Server preference: brotli > gzip.
fn negotiate(accept_encoding: &str, allowed: Algos) -> Option<Coding> {
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

    if allowed.brotli() && br_q > 0.0 {
        return Some(Coding::Brotli);
    }
    if allowed.gzip() && gz_q > 0.0 {
        return Some(Coding::Gzip);
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

fn gzip_compress(data: &[u8], level: GzipLevel, out: &mut Vec<u8>) -> std::io::Result<()> {
    let mut enc = flate2::write::GzEncoder::new(out, flate2::Compression::new(level.0.into()));
    enc.write_all(data)?;
    enc.finish()?;
    Ok(())
}

fn brotli_compress(data: &[u8], quality: BrotliQuality, out: &mut Vec<u8>) -> std::io::Result<()> {
    let params = brotli::enc::BrotliEncoderParams {
        quality: quality.0.into(),
        ..Default::default()
    };
    let mut reader = data;
    brotli::BrotliCompress(&mut reader, out, &params)?;
    Ok(())
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

// ---------------------------------------------------------------------------
// Compressing a response
// ---------------------------------------------------------------------------

/// A body at or above this size is compressed on the blocking pool: gzip-6 and brotli-4
/// take on the order of 100 µs per 16 KiB, too long to hold a tokio worker.
const INLINE_MAX_BYTES: usize = 16 * 1024;

/// One body to compress with one codec. Owns a reference to the body, so it can move to
/// the blocking pool.
struct Job {
    body: Bytes,
    algo: Algo,
}

impl Job {
    fn runs_inline(&self) -> bool {
        let slow = matches!(self.algo, Algo::Brotli(q) if q.0 >= BrotliQuality::SLOW);
        self.body.len() < INLINE_MAX_BYTES && !slow
    }

    /// The compressed body, or `None` when compression failed (logged) or did not shrink
    /// the body.
    fn run(self) -> Option<Bytes> {
        let mut buf = COMPRESS_POOL
            .lock()
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.body.len() / 2 + 64));
        buf.clear();

        let compressed = match self.algo {
            Algo::Gzip(level) => gzip_compress(&self.body, level, &mut buf),
            Algo::Brotli(quality) => brotli_compress(&self.body, quality, &mut buf),
        };
        let buf = PooledCompressBuf(buf);
        match compressed {
            Ok(()) => (buf.0.len() < self.body.len()).then(|| Bytes::from_owner(buf)),
            Err(e) => {
                tracing::warn!(
                    target: "pyronova::server",
                    error = %e,
                    encoding = self.algo.header_value(),
                    "response compression failed; sending the body uncompressed"
                );
                None
            }
        }
    }
}

/// What compressing `data` takes, or `None` when it goes out as is: no settings (off), a
/// handler-set `Content-Encoding`, a type not worth compressing, a body under
/// `min_size`, or no coding the client and the app share.
fn plan(data: &ResponseData, accept_encoding: &str, settings: &Settings) -> Option<Job> {
    if accept_encoding.is_empty() || data.headers.contains_key("content-encoding") {
        return None;
    }
    if data.body.len() < settings.min_size as usize {
        return None;
    }
    // A handler's own `content-type` header is the type that goes out.
    let content_type = data
        .headers
        .get("content-type")
        .or_else(|| data.content_type.to_str().ok())?;
    if !is_compressible(content_type) {
        return None;
    }
    let coding = negotiate(accept_encoding, settings.algos)?;
    Some(Job {
        body: data.body.clone(),
        algo: coding.at(settings),
    })
}

fn apply(data: &mut ResponseData, compressed: Bytes, algo: Algo) {
    data.body = compressed;
    set_compression_headers(data.headers.as_map_mut(), algo.header_value());
}

/// `data` compressed for a client that sent `accept_encoding`, as `settings` (the app's;
/// `None` = off) allow. A large body is compressed on the blocking pool.
pub(crate) async fn compress(
    mut data: ResponseData,
    accept_encoding: &str,
    settings: Option<&Settings>,
) -> ResponseData {
    let Some(job) = settings.and_then(|s| plan(&data, accept_encoding, s)) else {
        return data;
    };
    let algo = job.algo;
    let compressed = if job.runs_inline() {
        job.run()
    } else {
        off_the_worker(job).await
    };
    if let Some(compressed) = compressed {
        apply(&mut data, compressed, algo);
    }
    data
}

async fn off_the_worker(job: Job) -> Option<Bytes> {
    let encoding = job.algo.header_value();
    match tokio::task::spawn_blocking(move || job.run()).await {
        Ok(compressed) => compressed,
        Err(e) => {
            tracing::warn!(
                target: "pyronova::server",
                error = %e,
                encoding,
                "response compression task did not finish; sending the body uncompressed"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ResponseHeaders;

    fn settings(min_size: i64, gzip: bool, brotli: bool) -> Settings {
        Settings::parse(min_size, gzip, brotli, 6, 4).unwrap()
    }

    fn json(body: impl Into<Bytes>) -> ResponseData {
        ResponseData {
            body: body.into(),
            content_type: HeaderValue::from_static("application/json"),
            status: hyper::StatusCode::OK,
            headers: ResponseHeaders::new(),
        }
    }

    /// `compress` on a current-thread runtime, as a TPC thread runs it.
    fn compressed(data: ResponseData, accept: &str, settings: Option<&Settings>) -> ResponseData {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(compress(data, accept, settings))
    }

    #[test]
    fn negotiate_picks_brotli_over_gzip() {
        assert_eq!(
            negotiate("gzip, deflate, br", Algos::Both),
            Some(Coding::Brotli)
        );
    }

    #[test]
    fn negotiate_gzip_only_when_brotli_disabled() {
        assert_eq!(negotiate("gzip, br", Algos::Gzip), Some(Coding::Gzip));
    }

    #[test]
    fn negotiate_respects_q_zero() {
        assert_eq!(negotiate("br;q=0, gzip", Algos::Both), Some(Coding::Gzip));
    }

    #[test]
    fn negotiate_wildcard() {
        assert_eq!(negotiate("*", Algos::Both), Some(Coding::Brotli));
    }

    #[test]
    fn negotiate_wildcard_q_zero_excludes() {
        // "*;q=0, gzip" — explicitly listed gzip wins, brotli excluded by wildcard
        assert_eq!(negotiate("*;q=0, gzip", Algos::Both), Some(Coding::Gzip));
    }

    #[test]
    fn negotiate_none_when_unsupported() {
        assert_eq!(negotiate("deflate, compress", Algos::Both), None);
    }

    #[test]
    fn negotiate_uppercase_q_param() {
        assert_eq!(negotiate("br;Q=0, gzip", Algos::Both), Some(Coding::Gzip));
    }

    #[test]
    fn negotiate_uppercase_algo_names() {
        assert_eq!(negotiate("BR, GZIP", Algos::Both), Some(Coding::Brotli));
    }

    #[test]
    fn negotiate_malformed_q_disables() {
        assert_eq!(negotiate("br;q=bad, gzip", Algos::Both), Some(Coding::Gzip));
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
    fn no_settings_is_noop() {
        let data = compressed(json(vec![b'x'; 2048]), "gzip, br", None);
        assert_eq!(data.body.len(), 2048);
        assert!(!data.headers.contains_key("content-encoding"));
    }

    #[test]
    fn enabled_compresses_json() {
        let payload = vec![b'x'; 4096];
        let data = compressed(
            json(payload.clone()),
            "br, gzip",
            Some(&settings(100, true, true)),
        );
        assert!(data.body.len() < payload.len());
        assert_eq!(data.headers.get("content-encoding").unwrap(), "br");
        assert_eq!(
            data.headers.get("vary").unwrap().to_ascii_lowercase(),
            "accept-encoding"
        );
    }

    #[test]
    fn large_body_compresses_off_the_worker() {
        let payload = vec![b'y'; INLINE_MAX_BYTES * 4];
        let job = plan(&json(payload.clone()), "gzip", &settings(100, true, false)).unwrap();
        assert!(!job.runs_inline());
        let data = compressed(
            json(payload.clone()),
            "gzip",
            Some(&settings(100, true, false)),
        );
        assert!(data.body.len() < payload.len());
        assert_eq!(data.headers.get("content-encoding").unwrap(), "gzip");
    }

    #[test]
    fn slow_brotli_quality_never_runs_inline() {
        let slow = Settings::parse(0, false, true, 6, 11).unwrap();
        let job = plan(&json(vec![b'z'; 1024]), "br", &slow).unwrap();
        assert!(!job.runs_inline());
        let fast = plan(&json(vec![b'z'; 1024]), "br", &settings(0, false, true)).unwrap();
        assert!(fast.runs_inline());
    }

    #[test]
    fn small_body_skipped() {
        let data = json("small");
        assert!(plan(&data, "gzip, br", &settings(512, true, true)).is_none());
    }

    #[test]
    fn binary_content_type_skipped() {
        let mut data = json(vec![0u8; 2048]);
        data.content_type = HeaderValue::from_static("image/png");
        assert!(plan(&data, "gzip, br", &settings(100, true, true)).is_none());
    }

    #[test]
    fn handler_content_encoding_preserved() {
        let mut data = json(vec![b'x'; 2048]);
        data.headers
            .as_map_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("identity"));
        let data = compressed(data, "gzip, br", Some(&settings(100, true, true)));
        assert_eq!(data.headers.get("Content-Encoding").unwrap(), "identity");
    }

    #[test]
    fn vary_merges_with_existing() {
        let mut data = json(vec![b'a'; 4096]);
        data.headers
            .as_map_mut()
            .insert(VARY, HeaderValue::from_static("Origin"));
        let data = compressed(data, "gzip", Some(&settings(100, true, true)));
        let vary = data.headers.get("Vary").unwrap().to_ascii_lowercase();
        assert!(vary.contains("origin"));
        assert!(vary.contains("accept-encoding"));
    }

    #[test]
    fn empty_accept_encoding_noop() {
        assert!(plan(&json(vec![b'x'; 2048]), "", &settings(100, true, true)).is_none());
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
            negotiate("br;\u{20ac}, gzip", Algos::Both),
            Some(Coding::Brotli)
        );
        assert_eq!(
            negotiate("br;q\u{20ac}", Algos::Brotli),
            Some(Coding::Brotli)
        );
    }

    #[test]
    fn levels_out_of_range_are_errors_not_clamped() {
        assert_eq!(
            Settings::parse(100, true, true, 42, 4),
            Err(SettingsError::GzipLevel(42))
        );
        assert_eq!(
            Settings::parse(100, true, true, 0, 4),
            Err(SettingsError::GzipLevel(0))
        );
        assert_eq!(
            Settings::parse(100, true, true, 6, 12),
            Err(SettingsError::BrotliQuality(12))
        );
        assert_eq!(
            Settings::parse(100, true, true, 6, -1),
            Err(SettingsError::BrotliQuality(-1))
        );
        assert_eq!(
            Settings::parse(-1, true, true, 6, 4),
            Err(SettingsError::MinSize(-1))
        );
    }

    #[test]
    fn no_algorithm_is_an_error() {
        assert_eq!(
            Settings::parse(100, false, false, 6, 4),
            Err(SettingsError::NoAlgorithm)
        );
    }

    #[test]
    fn range_edges_are_accepted() {
        let s = Settings::parse(0, true, false, 1, 0).unwrap();
        assert_eq!((s.gzip_level.0, s.brotli_quality.0), (1, 0));
        let s = Settings::parse(0, false, true, 9, 11).unwrap();
        assert_eq!((s.gzip_level.0, s.brotli_quality.0), (9, 11));
    }

    #[test]
    fn compress_helpers_report_success_as_a_result() {
        let data = vec![b'a'; 4096];
        let mut gz = Vec::new();
        gzip_compress(&data, GzipLevel(6), &mut gz).unwrap();
        assert!(!gz.is_empty() && gz.len() < data.len());
        let mut br = Vec::new();
        brotli_compress(&data, BrotliQuality(4), &mut br).unwrap();
        assert!(!br.is_empty() && br.len() < data.len());
    }
}
