use bytes::Bytes;
use dashmap::DashMap;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use percent_encoding::percent_decode_str;
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::io::AsyncReadExt;

/// Maximum size (in bytes) of a static file served out of memory.
/// Files larger than this are refused with 413 to avoid OOM on pathological
/// requests (multi-GB files in the static dir, etc.).
const MAX_STATIC_FILE_BYTES: u64 = 16 * 1024 * 1024; // 16 MiB

/// Soft cap on the in-memory static-file cache. Once the cache grows past
/// this many cumulative bytes, new files are served without being cached
/// (old entries stay — we don't evict). At 128 MiB the cap comfortably
/// holds any reasonable static site without letting a misconfigured root
/// blow the RSS budget.
const STATIC_CACHE_MAX_BYTES: u64 = 128 * 1024 * 1024;

// ─── Mounts, parsed once at registration ───────────────────────────────────

/// An absolute, symlink-resolved path to an existing directory.
#[derive(Debug, Clone)]
pub(crate) struct CanonicalDir(PathBuf);

impl CanonicalDir {
    fn resolve(dir: &str) -> Result<Self, StaticMountError> {
        let path = std::fs::canonicalize(dir).map_err(|source| StaticMountError::Root {
            dir: dir.to_string(),
            source,
        })?;
        if !path.is_dir() {
            return Err(StaticMountError::NotADirectory(path));
        }
        Ok(Self(path))
    }
}

/// A URL prefix served from a directory. The prefix always starts and ends with `/`,
/// so `/static/` never matches `/staticfoo`.
#[derive(Debug, Clone)]
pub(crate) struct StaticMount {
    prefix: String,
    root: CanonicalDir,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StaticMountError {
    #[error("static prefix {0:?} must start with '/'")]
    Prefix(String),
    #[error("static directory {dir:?} cannot be resolved: {source}")]
    Root { dir: String, source: io::Error },
    #[error("static directory {0:?} is not a directory")]
    NotADirectory(PathBuf),
}

impl StaticMount {
    pub(crate) fn new(prefix: &str, dir: &str) -> Result<Self, StaticMountError> {
        if !prefix.starts_with('/') {
            return Err(StaticMountError::Prefix(prefix.to_string()));
        }
        let prefix = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };
        Ok(Self {
            prefix,
            root: CanonicalDir::resolve(dir)?,
        })
    }

    /// The still-encoded part of `req_path` under this mount, if it is for this mount.
    fn relative<'a>(&self, req_path: &'a str) -> Option<&'a str> {
        req_path
            .strip_prefix(self.prefix.as_str())
            .filter(|rel| !rel.is_empty())
    }
}

// ─── Request errors ─────────────────────────────────────────────────────────

/// Why a static request was refused. "No such file" is not an error: it lets the
/// next mount, then routing, answer (usually 404).
#[derive(Debug, thiserror::Error)]
enum StaticError {
    #[error("request path contains an encoded NUL byte")]
    NulByte,
    #[cfg(not(unix))]
    #[error("request path is not valid UTF-8 after percent-decoding")]
    NotUtf8,
    #[error("request path climbs out of the static root with '..'")]
    Traversal,
    #[error("{path:?} resolves outside the static root {root:?}")]
    Escape { path: PathBuf, root: PathBuf },
    #[error("{path:?} is {len} bytes, over the {MAX_STATIC_FILE_BYTES}-byte static file limit")]
    TooLarge { path: PathBuf, len: u64 },
    #[error("{path:?}: {source}")]
    PermissionDenied { path: PathBuf, source: io::Error },
    #[error("{path:?} became a symlink after the containment check: {source}")]
    SymlinkSwapped { path: PathBuf, source: io::Error },
    #[error("{path:?}: {source}")]
    Io { path: PathBuf, source: io::Error },
}

impl StaticError {
    /// EACCES is 403, not 500: the file exists and the server is not allowed to read
    /// it, which is the same answer nginx gives, and it is an access decision about the
    /// resource rather than a fault in serving it.
    fn status(&self) -> StatusCode {
        match self {
            Self::NulByte => StatusCode::BAD_REQUEST,
            #[cfg(not(unix))]
            Self::NotUtf8 => StatusCode::BAD_REQUEST,
            Self::Traversal
            | Self::Escape { .. }
            | Self::PermissionDenied { .. }
            | Self::SymlinkSwapped { .. } => StatusCode::FORBIDDEN,
            Self::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Io { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The response body. File paths and OS errors stay in the server log.
    fn public_text(&self) -> &'static str {
        match self.status() {
            StatusCode::BAD_REQUEST => "bad request",
            StatusCode::FORBIDDEN => "forbidden",
            StatusCode::PAYLOAD_TOO_LARGE => "payload too large",
            _ => "internal server error",
        }
    }

    /// Client-triggerable refusals log at debug so a scanner cannot flood the log;
    /// problems with the files themselves are the operator's to see.
    fn log(&self) {
        match self {
            Self::Io { .. } => {
                tracing::error!(target: "pyronova::server", error = %self, "static file error")
            }
            Self::PermissionDenied { .. } | Self::SymlinkSwapped { .. } => {
                tracing::warn!(target: "pyronova::server", error = %self, "static file refused")
            }
            _ => {
                tracing::debug!(target: "pyronova::server", error = %self, "static request refused")
            }
        }
    }
}

/// `Ok(None)` when the path simply is not there (including a file used as a
/// directory), the typed error otherwise.
fn found<T>(path: &Path, result: io::Result<T>) -> Result<Option<T>, StaticError> {
    let source = match result {
        Ok(value) => return Ok(Some(value)),
        Err(source) => source,
    };
    let path = path.to_path_buf();
    match source.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => Ok(None),
        io::ErrorKind::PermissionDenied => Err(StaticError::PermissionDenied { path, source }),
        _ => Err(StaticError::Io { path, source }),
    }
}

// ─── Cache ──────────────────────────────────────────────────────────────────

/// Cache entry: the file bytes + precomputed content-type. We cache on
/// the canonical path so symlinks inside the static root resolve to the
/// same entry as their target.
#[derive(Clone)]
struct CachedFile {
    bytes: Bytes,
    content_type: &'static str,
}

fn cache() -> &'static DashMap<PathBuf, CachedFile> {
    static C: OnceLock<DashMap<PathBuf, CachedFile>> = OnceLock::new();
    C.get_or_init(DashMap::new)
}

fn cache_bytes() -> &'static std::sync::atomic::AtomicU64 {
    static B: OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
    B.get_or_init(|| std::sync::atomic::AtomicU64::new(0))
}

/// Populate the cache for future requests. Skip once the cumulative cache size has
/// crossed the soft cap — serving uncached is still correct, just pays the re-read
/// cost. We don't evict: static files rarely rotate, and an LRU would need locking
/// around every hit.
fn cache_insert(path: PathBuf, bytes: Bytes, content_type: &'static str) {
    use std::sync::atomic::Ordering::Relaxed;
    let len = bytes.len() as u64;
    // Atomically reserve space: fetch_add first, then check the new total.
    // If we overshoot the soft cap, roll back and skip caching.
    if cache_bytes().fetch_add(len, Relaxed) + len > STATIC_CACHE_MAX_BYTES {
        cache_bytes().fetch_sub(len, Relaxed);
        return;
    }
    // `insert` returns the entry it replaced. If a concurrent thread (or an earlier
    // request) already cached this key, our `fetch_add` double-counted: subtract the
    // replaced entry's bytes so the counter tracks live map contents. This sub is
    // balanced — `prev` was added by whoever inserted it — so it cannot underflow.
    if let Some(prev) = cache().insert(
        path,
        CachedFile {
            bytes,
            content_type,
        },
    ) {
        cache_bytes().fetch_sub(prev.bytes.len() as u64, Relaxed);
    }
}

/// Return the MIME type for a file extension. The extension is expected in
/// lowercase without a leading dot (e.g. `mime_from_ext("png")`). Falls
/// back to `application/octet-stream` for unknown extensions.
pub fn mime_from_ext(ext: &str) -> &'static str {
    match ext {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "pdf" => "application/pdf",
        "xml" => "application/xml; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "wasm" => "application/wasm",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

fn content_type(path: &Path) -> &'static str {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    mime_from_ext(&ext.to_ascii_lowercase())
}

// ─── Serving ────────────────────────────────────────────────────────────────

/// Serve `req_path` from the first mount that has it. `None` means no mount has the
/// file, so routing answers.
///
/// Per-request cost: the root is canonical from registration, so a request pays one
/// `canonicalize` (the candidate) instead of two. Decoding scans the relative path once
/// and allocates only if it contains `%`; the candidate `PathBuf` is the one allocation
/// the old `join` also made.
pub(crate) async fn try_static_file(
    req_path: &str,
    mounts: &[StaticMount],
) -> Option<Response<Full<Bytes>>> {
    for mount in mounts {
        let Some(rel) = mount.relative(req_path) else {
            continue;
        };
        match serve(&mount.root.0, rel).await {
            Ok(Some(response)) => return Some(response),
            Ok(None) => continue,
            Err(e) => {
                e.log();
                return Some(error_response(&e));
            }
        }
    }
    None
}

async fn serve(root: &Path, rel: &str) -> Result<Option<Response<Full<Bytes>>>, StaticError> {
    let candidate = candidate_path(root, rel)?;
    let Some(path) = canonical_within(root, &candidate).await? else {
        return Ok(None);
    };

    // Cache hit: reuse the shared Bytes (Arc clone, zero-copy). Benchmark-grade static
    // profiles hit the same 20 files from thousands of connections per second; without
    // this cache every request re-reads its file into a fresh Vec<u8>.
    if let Some(entry) = cache().get(&path) {
        return Ok(Some(ok_response_bytes(
            entry.content_type,
            entry.bytes.clone(),
        )));
    }

    let Some(bytes) = read_regular_file(&path).await? else {
        return Ok(None);
    };
    let ct = content_type(&path);
    cache_insert(path, bytes.clone(), ct);
    Ok(Some(ok_response_bytes(ct, bytes)))
}

/// `root` joined with the percent-decoded request segments. Only plain file names are
/// pushed, so neither `..` (literal or `%2e%2e`) nor an absolute segment (`%2f...`) can
/// move the path: `%2f` decodes to a separator and splits like a literal `/`.
fn candidate_path(root: &Path, rel: &str) -> Result<PathBuf, StaticError> {
    let decoded: std::borrow::Cow<'_, [u8]> = percent_decode_str(rel).into();
    if decoded.contains(&0) {
        return Err(StaticError::NulByte);
    }
    let mut path = root.to_path_buf();
    for segment in decoded.split(|&b| b == b'/') {
        match segment {
            b"" | b"." => {}
            b".." => return Err(StaticError::Traversal),
            name => path.push(file_name(name)?),
        }
    }
    Ok(path)
}

#[cfg(unix)]
fn file_name(bytes: &[u8]) -> Result<&OsStr, StaticError> {
    use std::os::unix::ffi::OsStrExt;
    Ok(OsStr::from_bytes(bytes))
}

#[cfg(not(unix))]
fn file_name(bytes: &[u8]) -> Result<&OsStr, StaticError> {
    std::str::from_utf8(bytes)
        .map(OsStr::new)
        .map_err(|_| StaticError::NotUtf8)
}

/// Resolve symlinks and check the result is still under `root`. An attacker who plants
/// a symlink inside the root pointing at `/etc/passwd` is refused here.
async fn canonical_within(root: &Path, candidate: &Path) -> Result<Option<PathBuf>, StaticError> {
    let Some(path) = found(candidate, tokio::fs::canonicalize(candidate).await)? else {
        return Ok(None);
    };
    if !path.starts_with(root) {
        return Err(StaticError::Escape {
            path,
            root: root.to_path_buf(),
        });
    }
    Ok(Some(path))
}

/// Read a regular file of at most `MAX_STATIC_FILE_BYTES`; `None` if it is gone or
/// not a regular file (a directory).
///
/// Open once and derive metadata from the fd, so the size check and the read operate
/// on the same inode (no metadata-then-read TOCTOU).
///
/// O_NOFOLLOW: `canonical_within` followed symlinks to decide containment. Between that
/// and the open, an attacker with write access to the final path segment could swap
/// the file for a symlink pointing anywhere on disk; with O_NOFOLLOW the open refuses
/// a symlink at the last component (ELOOP). Legitimate symlinks inside the root were
/// already resolved by `canonical_within`.
async fn read_regular_file(path: &Path) -> Result<Option<Bytes>, StaticError> {
    let opened = match open_no_follow(path).await {
        Err(source) if is_symlink_refusal(&source) => {
            return Err(StaticError::SymlinkSwapped {
                path: path.to_path_buf(),
                source,
            })
        }
        opened => found(path, opened)?,
    };
    let Some(file) = opened else {
        return Ok(None);
    };
    let Some(metadata) = found(path, file.metadata().await)? else {
        return Ok(None);
    };
    if !metadata.is_file() {
        return Ok(None);
    }
    if metadata.len() > MAX_STATIC_FILE_BYTES {
        return Err(StaticError::TooLarge {
            path: path.to_path_buf(),
            len: metadata.len(),
        });
    }

    // Belt + braces: even if the metadata-reported size was stale for any reason,
    // `take()` enforces the byte cap on the read itself.
    let mut contents = Vec::with_capacity(metadata.len() as usize);
    let read = file
        .take(MAX_STATIC_FILE_BYTES)
        .read_to_end(&mut contents)
        .await;
    Ok(found(path, read)?.map(|_| Bytes::from(contents)))
}

#[cfg(unix)]
async fn open_no_follow(path: &Path) -> io::Result<tokio::fs::File> {
    tokio::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .await
}

#[cfg(unix)]
fn is_symlink_refusal(e: &io::Error) -> bool {
    e.raw_os_error() == Some(libc::ELOOP)
}

#[cfg(not(unix))]
async fn open_no_follow(path: &Path) -> io::Result<tokio::fs::File> {
    tokio::fs::File::open(path).await
}

#[cfg(not(unix))]
fn is_symlink_refusal(_: &io::Error) -> bool {
    false
}

// ─── Responses ──────────────────────────────────────────────────────────────
//
// Built from constant header values + a fixed body, so they cannot fail in practice
// and `.expect` is used instead of bubbling a Result. `nosniff` is added to every
// response to prevent MIME-type sniffing attacks when users upload content into the
// static directory.

fn error_response(e: &StaticError) -> Response<Full<Bytes>> {
    Response::builder()
        .status(e.status())
        .header("server", crate::response::SERVER_HEADER)
        .header("x-content-type-options", "nosniff")
        .body(Full::new(Bytes::from_static(e.public_text().as_bytes())))
        .expect("static error response: constant components are valid")
}

fn ok_response_bytes(content_type: &'static str, bytes: Bytes) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .header("server", crate::response::SERVER_HEADER)
        .header("x-content-type-options", "nosniff")
        .body(Full::new(bytes))
        .expect("static ok response: constant headers + validated ct are valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mime_html() {
        assert_eq!(mime_from_ext("html"), "text/html; charset=utf-8");
        assert_eq!(mime_from_ext("htm"), "text/html; charset=utf-8");
    }

    #[test]
    fn mime_js_css() {
        assert_eq!(mime_from_ext("css"), "text/css; charset=utf-8");
        assert_eq!(mime_from_ext("js"), "application/javascript; charset=utf-8");
        assert_eq!(
            mime_from_ext("mjs"),
            "application/javascript; charset=utf-8"
        );
    }

    #[test]
    fn mime_images() {
        assert_eq!(mime_from_ext("png"), "image/png");
        assert_eq!(mime_from_ext("jpg"), "image/jpeg");
        assert_eq!(mime_from_ext("jpeg"), "image/jpeg");
        assert_eq!(mime_from_ext("gif"), "image/gif");
        assert_eq!(mime_from_ext("svg"), "image/svg+xml");
        assert_eq!(mime_from_ext("webp"), "image/webp");
        assert_eq!(mime_from_ext("ico"), "image/x-icon");
    }

    #[test]
    fn mime_fonts() {
        assert_eq!(mime_from_ext("woff"), "font/woff");
        assert_eq!(mime_from_ext("woff2"), "font/woff2");
        assert_eq!(mime_from_ext("ttf"), "font/ttf");
        assert_eq!(mime_from_ext("otf"), "font/otf");
    }

    #[test]
    fn mime_application() {
        assert_eq!(mime_from_ext("json"), "application/json; charset=utf-8");
        assert_eq!(mime_from_ext("pdf"), "application/pdf");
        assert_eq!(mime_from_ext("xml"), "application/xml; charset=utf-8");
        assert_eq!(mime_from_ext("wasm"), "application/wasm");
        assert_eq!(mime_from_ext("map"), "application/json");
    }

    #[test]
    fn mime_unknown_fallback() {
        assert_eq!(mime_from_ext("xyz"), "application/octet-stream");
        assert_eq!(mime_from_ext(""), "application/octet-stream");
        assert_eq!(mime_from_ext("bin"), "application/octet-stream");
    }

    // ── Security / path-traversal tests ────────────────────────────

    async fn write_file(p: &std::path::Path, bytes: &[u8]) {
        tokio::fs::write(p, bytes).await.expect("test setup: write");
    }

    #[tokio::test]
    async fn serves_a_file_inside_the_static_root() {
        let tmp = tempdir();
        let root = tmp.path();
        write_file(&root.join("ok.txt"), b"hello").await;

        let dirs = vec![StaticMount::new("/static", &root.to_string_lossy()).unwrap()];
        let resp = try_static_file("/static/ok.txt", &dirs).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
    }

    #[tokio::test]
    async fn rejects_symlink_escape() {
        let tmp = tempdir();
        let root = tmp.path();
        // A secret lives OUTSIDE the static root.
        let secret_dir = tempdir();
        write_file(&secret_dir.path().join("secret.txt"), b"SHHH").await;

        // Attacker plants a symlink inside root pointing at the secret.
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            secret_dir.path().join("secret.txt"),
            root.join("escape.txt"),
        )
        .unwrap();

        let dirs = vec![StaticMount::new("/static", &root.to_string_lossy()).unwrap()];
        let resp = try_static_file("/static/escape.txt", &dirs).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn rejects_parent_path_literal() {
        let tmp = tempdir();
        let dirs = vec![StaticMount::new("/static", &tmp.path().to_string_lossy()).unwrap()];
        let resp = try_static_file("/static/../etc/passwd", &dirs)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn refuses_oversized_files() {
        let tmp = tempdir();
        let big = tmp.path().join("big.bin");
        let big_len = MAX_STATIC_FILE_BYTES + 1;
        {
            let file = std::fs::File::create(&big).unwrap();
            file.set_len(big_len).unwrap();
            // `file` drops here, ensuring metadata is flushed before the
            // async try_static_file reads it.
        }

        let dirs = vec![StaticMount::new("/static", &tmp.path().to_string_lossy()).unwrap()];
        let resp = try_static_file("/static/big.bin", &dirs).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn non_existent_file_returns_none() {
        let tmp = tempdir();
        let dirs = vec![StaticMount::new("/static", &tmp.path().to_string_lossy()).unwrap()];
        assert!(try_static_file("/static/missing.txt", &dirs)
            .await
            .is_none());
    }

    // Minimal tempdir helper: avoid pulling in a dep just for tests.
    // Uses a process-global atomic counter (not a clock) so two calls in
    // rapid succession are guaranteed distinct paths.
    use std::sync::atomic::{AtomicU64, Ordering};
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tempdir() -> TempDir {
        let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut base = std::env::temp_dir();
        base.push(format!(
            "pyronova-static-fs-test-{}-{}",
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_dir_all(&base); // clean stale from prior crashed run
        std::fs::create_dir_all(&base).unwrap();
        TempDir { path: base }
    }
    struct TempDir {
        path: std::path::PathBuf,
    }
    impl TempDir {
        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    // ── Review cced8c2 M1d-4 ───────────────────────────────────────

    #[test]
    fn candidate_path_decodes_and_refuses_encoded_traversal() {
        let root = Path::new("/srv/public");
        assert_eq!(
            candidate_path(root, "my%20file.txt").unwrap(),
            root.join("my file.txt")
        );
        for rel in [
            "%2e%2e/x",
            "%2E%2E/x",
            "..%2fx",
            "a/%2e%2e%2f%2e%2e/x",
            "a/../x",
        ] {
            assert!(
                matches!(candidate_path(root, rel), Err(StaticError::Traversal)),
                "{rel}"
            );
        }
    }

    #[test]
    fn candidate_path_cannot_be_replaced_by_an_absolute_segment() {
        let root = Path::new("/srv/public");
        assert_eq!(
            candidate_path(root, "%2fetc%2fpasswd").unwrap(),
            root.join("etc/passwd")
        );
        assert_eq!(candidate_path(root, "./a//b").unwrap(), root.join("a/b"));
    }

    #[test]
    fn candidate_path_refuses_nul() {
        assert!(matches!(
            candidate_path(Path::new("/srv"), "a%00b"),
            Err(StaticError::NulByte)
        ));
    }

    #[test]
    fn mount_rejects_bad_prefix_and_non_directory_root() {
        let tmp = tempdir();
        let file = tmp.path().join("f.txt");
        std::fs::write(&file, b"x").unwrap();
        let root = tmp.path().to_string_lossy().to_string();

        assert!(matches!(
            StaticMount::new("static", &root),
            Err(StaticMountError::Prefix(_))
        ));
        assert!(matches!(
            StaticMount::new("/s", &file.to_string_lossy()),
            Err(StaticMountError::NotADirectory(_))
        ));
        assert!(matches!(
            StaticMount::new("/s", &tmp.path().join("missing").to_string_lossy()),
            Err(StaticMountError::Root { .. })
        ));
        let mount = StaticMount::new("/s", &root).unwrap();
        assert_eq!(mount.relative("/s/a.txt"), Some("a.txt"));
        assert_eq!(mount.relative("/sa.txt"), None);
        assert_eq!(mount.relative("/s/"), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn io_errors_are_not_reported_as_missing() {
        let tmp = tempdir();
        let root = tmp.path();
        std::os::unix::fs::symlink("loop", root.join("loop")).unwrap();
        let dirs = vec![StaticMount::new("/static", &root.to_string_lossy()).unwrap()];
        let resp = try_static_file("/static/loop", &dirs).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
