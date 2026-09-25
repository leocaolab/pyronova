//! Pyronova logging engine — zero-cost tracing with non-blocking I/O.
//!
//! Provides:
//! - `init_logger`: installs (first call) or reconfigures (later calls) the process-wide
//!   tracing subscriber, writing through a non-blocking writer
//! - `emit_python_log`: receives Python `logging` calls via FFI, routes to tracing
//!
//! Key: uses `tracing-appender::non_blocking` to avoid StdoutLock contention.
//! Without this, 220k QPS access log would starve Tokio worker threads on
//! the global stdout mutex.

use std::str::FromStr;

use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, reload, EnvFilter, Layer, Registry};

// ---------------------------------------------------------------------------
// Configuration, parsed at the Python edge
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Text,
    Json,
}

impl FromStr for LogFormat {
    type Err = LoggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(LoggerError::Format(s.to_string())),
        }
    }
}

/// The levels `LogConfig["level"]` documents, plus Python's spelling `WARNING`.
/// Stricter than `LevelFilter::from_str`, which also takes `""` and `0`–`5`, and far
/// stricter than an `EnvFilter` directive, where any unknown word is a *target* name.
fn parse_level(s: &str) -> Result<LevelFilter, LoggerError> {
    match s.to_ascii_uppercase().as_str() {
        "OFF" => Ok(LevelFilter::OFF),
        "ERROR" => Ok(LevelFilter::ERROR),
        "WARN" | "WARNING" => Ok(LevelFilter::WARN),
        "INFO" => Ok(LevelFilter::INFO),
        "DEBUG" => Ok(LevelFilter::DEBUG),
        "TRACE" => Ok(LevelFilter::TRACE),
        _ => Err(LoggerError::Level(s.to_string())),
    }
}

#[derive(Debug, Clone, Copy)]
struct LoggerConfig {
    level: LevelFilter,
    access_log: bool,
    format: LogFormat,
}

impl LoggerConfig {
    fn parse(level: &str, access_log: bool, format: &str) -> Result<Self, LoggerError> {
        Ok(Self {
            level: parse_level(level)?,
            access_log,
            format: format.parse()?,
        })
    }

    fn filter(&self) -> Result<EnvFilter, LoggerError> {
        let directives = if self.access_log {
            self.level.to_string()
        } else {
            format!("{},pyronova::access=off", self.level)
        };
        EnvFilter::try_new(&directives).map_err(|source| LoggerError::Filter { directives, source })
    }
}

#[derive(Debug, thiserror::Error)]
enum LoggerError {
    #[error("invalid log level {0:?}: expected one of OFF, ERROR, WARN, INFO, DEBUG, TRACE")]
    Level(String),
    #[error("invalid log format {0:?}: expected \"text\" or \"json\"")]
    Format(String),
    #[error("invalid log filter {directives:?}: {source}")]
    Filter {
        directives: String,
        source: tracing_subscriber::filter::ParseError,
    },
    #[error(
        "could not install the pyronova tracing subscriber; another global subscriber \
         or `log` logger is already set: {0}"
    )]
    Install(#[from] tracing_subscriber::util::TryInitError),
    #[error("could not reconfigure the pyronova tracing subscriber: {0}")]
    Reload(#[from] reload::Error),
}

impl From<LoggerError> for PyErr {
    fn from(e: LoggerError) -> Self {
        match e {
            LoggerError::Level(_) | LoggerError::Format(_) | LoggerError::Filter { .. } => {
                PyValueError::new_err(e.to_string())
            }
            LoggerError::Install(_) | LoggerError::Reload(_) => {
                PyRuntimeError::new_err(e.to_string())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The installed subscriber
// ---------------------------------------------------------------------------

type FilterLayer = reload::Layer<EnvFilter, Registry>;
type Filtered = Layered<FilterLayer, Registry>;
type FmtLayer = Box<dyn Layer<Filtered> + Send + Sync>;

/// The process-wide subscriber, installed by the first `init_logger` call.
///
/// tracing allows one global subscriber per process, but one process can run several
/// apps (TestClient, embedding), each with its own config. The filter and the format
/// layer sit behind `reload` handles so a later `init_logger` really applies its config
/// instead of silently keeping the first one.
///
/// The guard MUST outlive the writer — if dropped, the background I/O thread stops and
/// every subsequent log line is lost — so both live here for the life of the process.
struct Installed {
    /// The level the filter currently applies; workers gate their Python logging on it.
    level: LevelFilter,
    filter: reload::Handle<EnvFilter, Registry>,
    fmt: reload::Handle<FmtLayer, Filtered>,
    writer: tracing_appender::non_blocking::NonBlocking,
    _guard: tracing_appender::non_blocking::WorkerGuard,
}

static LOGGER: Mutex<Option<Installed>> = Mutex::new(None);

fn fmt_layer(format: LogFormat, writer: tracing_appender::non_blocking::NonBlocking) -> FmtLayer {
    match format {
        LogFormat::Json => fmt::layer().with_writer(writer).json().boxed(),
        LogFormat::Text => fmt::layer()
            .with_writer(writer)
            .with_target(true)
            .with_ansi(true)
            .boxed(),
    }
}

fn install(config: &LoggerConfig) -> Result<Installed, LoggerError> {
    // Non-blocking writer: all log I/O happens on a dedicated background thread.
    // Tokio workers never block on stderr — they just push into a channel.
    let (writer, guard) = tracing_appender::non_blocking(std::io::stderr());
    let (filter_layer, filter) = reload::Layer::new(config.filter()?);
    let (format_layer, fmt) = reload::Layer::new(fmt_layer(config.format, writer.clone()));
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(format_layer)
        .try_init()?;
    Ok(Installed {
        level: config.level,
        filter,
        fmt,
        writer,
        _guard: guard,
    })
}

fn reconfigure(installed: &mut Installed, config: &LoggerConfig) -> Result<(), LoggerError> {
    installed.filter.reload(config.filter()?)?;
    installed
        .fmt
        .reload(fmt_layer(config.format, installed.writer.clone()))?;
    installed.level = config.level;
    Ok(())
}

fn apply(config: &LoggerConfig) -> Result<(), LoggerError> {
    let mut slot = LOGGER.lock();
    match slot.as_mut() {
        Some(installed) => reconfigure(installed, config),
        None => {
            *slot = Some(install(config)?);
            Ok(())
        }
    }
}

/// Initialize the Rust tracing engine, or reconfigure it if already initialized.
///
/// - `level`: "OFF", "ERROR", "WARN" (or "WARNING"), "INFO", "DEBUG", "TRACE" — any case
/// - `access_log`: if false, suppresses all `pyronova::access` target logs
/// - `format`: "text" (human-readable) or "json" (structured)
///
/// Raises `ValueError` on an unknown level or format, and `RuntimeError` if the
/// subscriber cannot be installed (a foreign global subscriber already holds the slot).
///
/// Hot-path cost: the filter and format layers sit behind `reload`, so an event that
/// passes the filter takes a few uncontended `RwLock` reads on top of formatting and
/// the channel send. A filtered-out event is rejected by the cached callsite interest
/// and never reaches a lock.
#[pyfunction]
#[pyo3(signature = (level, access_log, format))]
pub fn init_logger(level: &str, access_log: bool, format: &str) -> PyResult<()> {
    let config = LoggerConfig::parse(level, access_log, format)?;
    apply(&config)?;
    tracing::info!(
        target: "pyronova::server",
        level = %config.level,
        access_log = config.access_log,
        format = ?config.format,
        "Pyronova tracing engine initialized"
    );
    Ok(())
}

/// The Python `logging` level matching the level `init_logger` applied, or `None` before
/// any `init_logger`. A worker's bootstrap sets its root logger to it, so records below
/// the threshold are dropped before formatting or crossing into Rust. One source for the
/// level: what the main interpreter parsed, never re-read from the environment.
#[pyfunction]
pub fn _python_log_level() -> Option<i64> {
    let level = LOGGER.lock().as_ref()?.level;
    Some(match level {
        LevelFilter::OFF => py_level::CRITICAL + 10,
        LevelFilter::ERROR => py_level::ERROR,
        LevelFilter::WARN => py_level::WARNING,
        LevelFilter::INFO => py_level::INFO,
        // Python has no TRACE: DEBUG records are the finest it emits.
        _ => py_level::DEBUG,
    })
}

// ---------------------------------------------------------------------------
// Python → Rust log bridge
// ---------------------------------------------------------------------------

/// Python's standard `logging` level numbers.
mod py_level {
    pub const DEBUG: i64 = 10;
    pub const INFO: i64 = 20;
    pub const WARNING: i64 = 30;
    pub const ERROR: i64 = 40;
    pub const CRITICAL: i64 = 50;
}

/// The tracing level a Python record lands on. Python levels are an open integer
/// scale (`logging.addLevelName(25, "NOTICE")`), so a record maps to the highest
/// standard threshold it reaches: 25 is INFO, 45 is ERROR, 5 is TRACE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl AppLogLevel {
    fn from_levelno(levelno: i64) -> Self {
        match levelno {
            n if n >= py_level::ERROR => Self::Error,
            n if n >= py_level::WARNING => Self::Warn,
            n if n >= py_level::INFO => Self::Info,
            n if n >= py_level::DEBUG => Self::Debug,
            _ => Self::Trace,
        }
    }
}

/// Dispatch a Python log record to the matching compile-time tracing macro.
///
/// This `macro_rules!` expands *inline* at every call site, so each `level`
/// branch remains a distinct static tracing callsite — `EnvFilter` keeps its
/// near-zero-cost skip. `emit_python_log` serves every interpreter (main and
/// sub-interpreter workers).
///
/// `$level` is an `AppLogLevel`; `$name`/`$pathname`/`$message` are formatted via
/// `Display`; `$wid` (an `Option<usize>`: no field when `None`) and `$lineno`
/// are recorded as integer fields.
macro_rules! dispatch_python_log {
    ($level:expr, $wid:expr, $name:expr, $pathname:expr, $lineno:expr, $message:expr $(,)?) => {
        match $level {
            AppLogLevel::Trace => {
                tracing::trace!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            AppLogLevel::Debug => {
                tracing::debug!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            AppLogLevel::Info => {
                tracing::info!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            AppLogLevel::Warn => {
                tracing::warn!(
                    target: "pyronova::app",
                    worker = $wid,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            AppLogLevel::Error => {
                tracing::error!(
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

/// Receive a Python logging record and route it through Rust tracing.
///
/// Called from `PyronovaRustHandler.emit()` in each interpreter (main + sub-interpreters)
/// with the record's numeric `levelno`. The actual filtering is done by `EnvFilter`.
/// `worker_id=None` (the main interpreter) records no `worker` field at all, so a
/// main-interpreter line can't pass for worker 0's (Layer 2, E2E-11).
#[pyfunction]
#[pyo3(signature = (levelno, name, message, pathname, lineno, worker_id=None))]
pub fn emit_python_log(
    levelno: i64,
    name: &str,
    message: &str,
    pathname: &str,
    lineno: u32,
    worker_id: Option<usize>,
) {
    dispatch_python_log!(
        AppLogLevel::from_levelno(levelno),
        worker_id,
        name,
        pathname,
        lineno,
        message
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levelno_maps_by_threshold() {
        assert_eq!(AppLogLevel::from_levelno(0), AppLogLevel::Trace);
        assert_eq!(AppLogLevel::from_levelno(9), AppLogLevel::Trace);
        assert_eq!(AppLogLevel::from_levelno(10), AppLogLevel::Debug);
        assert_eq!(AppLogLevel::from_levelno(20), AppLogLevel::Info);
        assert_eq!(AppLogLevel::from_levelno(25), AppLogLevel::Info);
        assert_eq!(AppLogLevel::from_levelno(30), AppLogLevel::Warn);
        assert_eq!(AppLogLevel::from_levelno(40), AppLogLevel::Error);
        assert_eq!(AppLogLevel::from_levelno(50), AppLogLevel::Error);
        assert_eq!(AppLogLevel::from_levelno(1000), AppLogLevel::Error);
    }

    #[test]
    fn level_accepts_documented_names_only() {
        assert_eq!(parse_level("warning").unwrap(), LevelFilter::WARN);
        assert_eq!(parse_level("Off").unwrap(), LevelFilter::OFF);
        for bad in ["", "3", "verbose", "pyronova=debug"] {
            assert!(
                matches!(parse_level(bad), Err(LoggerError::Level(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn format_is_closed() {
        assert_eq!("JSON".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("text".parse::<LogFormat>().unwrap(), LogFormat::Text);
        assert!(matches!(
            "yaml".parse::<LogFormat>(),
            Err(LoggerError::Format(_))
        ));
    }

    #[test]
    fn access_log_off_adds_its_directive() {
        let config = LoggerConfig::parse("info", false, "text").unwrap();
        assert!(config
            .filter()
            .unwrap()
            .to_string()
            .contains("pyronova::access=off"));
    }
}
