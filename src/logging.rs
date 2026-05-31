//! Pyronova logging engine — zero-cost tracing with non-blocking I/O.
//!
//! Provides:
//! - `init_logger`: configures tracing-subscriber with non-blocking writer
//! - `emit_python_log`: receives Python `logging` calls via FFI, routes to tracing
//!
//! Key: uses `tracing-appender::non_blocking` to avoid StdoutLock contention.
//! Without this, 220k QPS access log would starve Tokio worker threads on
//! the global stdout mutex.

use std::sync::OnceLock;

use pyo3::prelude::*;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Global singleton for the non-blocking writer + its WorkerGuard.
///
/// The guard MUST outlive the writer — if dropped, the background I/O
/// thread stops and every subsequent log line is silently lost. Storing
/// the pair atomically in one `OnceLock` is what makes init safe under
/// concurrent `init_logger()` calls: the first caller's pair wins, the
/// loser's pair is dropped together (its guard AND writer, never split).
///
/// Previous design stored only the guard in `OnceLock` and created a
/// fresh `(writer, guard)` tuple on every call; a racing caller could
/// win `try_init()` with its own writer but lose `NB_GUARD.set()`,
/// orphaning the writer because its guard was dropped.
struct LoggerState {
    nb_writer: tracing_appender::non_blocking::NonBlocking,
    _guard: tracing_appender::non_blocking::WorkerGuard,
}

static LOGGER: OnceLock<LoggerState> = OnceLock::new();

/// Dispatch a Python log record to the matching compile-time tracing macro.
///
/// This `macro_rules!` expands *inline* at every call site, so each `level`
/// branch remains a distinct static tracing callsite — `EnvFilter` keeps its
/// near-zero-cost skip. Extracted so the main-interpreter path
/// (`emit_python_log`) and the sub-interpreter C-FFI bridge
/// (`python::interp`) can never drift apart.
///
/// `$level` must be `&str`; `$name`/`$pathname`/`$message` are formatted via
/// `Display`; `$wid`/`$lineno` are recorded as integer fields.
macro_rules! dispatch_python_log {
    ($level:expr, $wid:expr, $name:expr, $pathname:expr, $lineno:expr, $message:expr $(,)?) => {
        match $level {
            "DEBUG" => {
                tracing::debug!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            "INFO" => {
                tracing::info!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            "WARNING" => {
                tracing::warn!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            "ERROR" | "CRITICAL" => {
                tracing::error!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            _ => {
                tracing::trace!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
        }
    };
}
pub(crate) use dispatch_python_log;

/// Initialize the Rust tracing engine. Called once at Pyronova startup.
///
/// - `level`: filter string — "OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"
/// - `access_log`: if false, suppresses all `pyronova::access` target logs
/// - `format`: "json" for structured output, anything else for human-readable text
#[pyfunction]
#[pyo3(signature = (level, access_log, format))]
pub fn init_logger(level: String, access_log: bool, format: String) -> PyResult<()> {
    // Validate the level string. `EnvFilter::new` uses `parse_lossy`, which
    // silently discards invalid directives — a malformed level can yield an
    // empty filter that lets everything through, with no error to the caller.
    // `try_new` surfaces the parse error so we can fall back to a sane "info"
    // default and emit a visible warning instead of filtering unpredictably.
    let mut filter = EnvFilter::try_new(&level).unwrap_or_else(|err| {
        eprintln!(
            "[pyronova] init_logger: invalid log level filter {level:?} ({err}); \
             falling back to \"info\""
        );
        EnvFilter::new("info")
    });

    // Suppress access log target when disabled
    if !access_log {
        // This is a compile-time constant directive — a parse failure here is a
        // programming error, not a runtime condition. Panic loudly rather than
        // silently leaving access logs enabled and breaking the access_log=false
        // contract.
        let directive = "pyronova::access=off"
            .parse()
            .expect("hardcoded log directive 'pyronova::access=off' must be a valid EnvFilter directive");
        filter = filter.add_directive(directive);
    }

    // Non-blocking writer: all log I/O happens on a dedicated background thread.
    // Tokio workers never block on stdout — they just push into an MPSC channel.
    // get_or_init keeps (writer, guard) atomic: races discard both together,
    // never split, so `try_init()` below always binds to a live writer.
    let nb_writer = LOGGER
        .get_or_init(|| {
            let (w, guard) = tracing_appender::non_blocking(std::io::stderr());
            LoggerState {
                nb_writer: w,
                _guard: guard,
            }
        })
        .nb_writer
        .clone();

    let result = if format.to_lowercase() == "json" {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(nb_writer)
                    .json(),
            )
            .try_init()
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(nb_writer)
                    .with_target(true)
                    .with_ansi(true),
            )
            .try_init()
    };

    if result.is_ok() {
        tracing::info!(
            target: "pyronova::server",
            level = %level,
            access_log = access_log,
            format = %format,
            "Pyronova tracing engine initialized"
        );
    } else if let Err(e) = result {
        // Pre-fix this branch was entirely silent — caller got Ok(())
        // believing logging was operational, but every log call then
        // silently no-op'd because a foreign subscriber held the slot
        // (arc finding logging-1). Print directly to stderr (we can't
        // use tracing — that's what failed) so the failure is at least
        // observable in startup output. Still return Ok because hot
        // reload + tests legitimately hit this path.
        eprintln!(
            "[pyronova] init_logger: tracing subscriber already set ({e}); \
             pyronova logging is INACTIVE — log calls from this process \
             will route to whatever subscriber installed first."
        );
    }

    Ok(())
}

/// Receive a Python logging record and route it through Rust tracing.
///
/// Called from `PyronovaRustHandler.emit()` in each interpreter (main + sub-interpreters).
/// The actual filtering is done by `EnvFilter` — Python side sets level=DEBUG
/// to let everything through, Rust decides what to keep.
#[pyfunction]
#[pyo3(signature = (level, name, message, pathname, lineno, worker_id=None))]
pub fn emit_python_log(
    level: String,
    name: String,
    message: String,
    pathname: String,
    lineno: u32,
    worker_id: Option<usize>,
) -> PyResult<()> {
    let wid = worker_id.unwrap_or(0);

    // Dispatch to compile-time tracing macros via match. The macro expands
    // inline, so each branch remains a separate static callsite — EnvFilter
    // can skip at near-zero cost.
    // Normalize to uppercase so the dispatch match (which only has uppercase
    // arms) categorizes lowercase/mixed-case levels correctly. Python's
    // logging.addLevelName() can register lowercase custom levels; without
    // this, those fall through to the TRACE arm and are silently dropped by
    // a typical EnvFilter (e.g. RUST_LOG=debug excludes TRACE).
    dispatch_python_log!(level.to_uppercase().as_str(), wid, name, pathname, lineno, message);

    Ok(())
}
