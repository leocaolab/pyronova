//! The process's tracing subscriber (`init_logger`), and the bridge Python `logging`
//! records cross into it (`emit_python_log`).
//!
//! Every line is written by a background thread (`tracing_appender::non_blocking`): at
//! hundreds of thousands of access-log lines a second, Tokio workers writing stderr
//! themselves would serialize on its lock.

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

/// The minimum level of what is logged. The one parser of a level's name: Python's
/// `enable_logging(level=)` and `LogConfig["level"]` go through `LogLevel.parse`.
#[pyclass(module = "pyronova.engine", eq, eq_int, frozen, hash)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[pymethods]
impl LogLevel {
    /// The level `name` spells, in any case: `OFF`, `ERROR`, `WARN` (or `WARNING`), `INFO`,
    /// `DEBUG`, `TRACE`. Anything else raises `ValueError`.
    #[staticmethod]
    fn parse(name: &str) -> PyResult<Self> {
        Ok(name.parse::<LogLevel>()?)
    }

    fn __str__(&self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }
}

/// The levels `LogConfig["level"]` documents, plus Python's spelling `WARNING`.
/// Stricter than `LevelFilter::from_str`, which also takes `""` and `0`–`5`, and far
/// stricter than an `EnvFilter` directive, where any unknown word is a *target* name.
impl FromStr for LogLevel {
    type Err = LoggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "OFF" => Ok(Self::Off),
            "ERROR" => Ok(Self::Error),
            "WARN" | "WARNING" => Ok(Self::Warn),
            "INFO" => Ok(Self::Info),
            "DEBUG" => Ok(Self::Debug),
            "TRACE" => Ok(Self::Trace),
            _ => Err(LoggerError::Level(s.to_string())),
        }
    }
}

impl LogLevel {
    /// A `level=` argument: a `LogLevel`, or its name.
    fn from_arg(arg: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(level) = arg.cast::<LogLevel>() {
            return Ok(*level.get());
        }
        let name: &str = arg.extract().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(format!(
                "level must be a pyronova.engine.LogLevel or a str, got {}",
                arg.get_type()
                    .name()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|_| "an object".into())
            ))
        })?;
        Ok(name.parse::<LogLevel>()?)
    }

    fn filter(self) -> LevelFilter {
        match self {
            Self::Off => LevelFilter::OFF,
            Self::Error => LevelFilter::ERROR,
            Self::Warn => LevelFilter::WARN,
            Self::Info => LevelFilter::INFO,
            Self::Debug => LevelFilter::DEBUG,
            Self::Trace => LevelFilter::TRACE,
        }
    }

    /// The Python `logging` level that lets through what this level logs.
    fn python_level(self) -> i64 {
        match self {
            Self::Off => py_level::CRITICAL + 10,
            Self::Error => py_level::ERROR,
            Self::Warn => py_level::WARNING,
            Self::Info => py_level::INFO,
            // Python has no TRACE: DEBUG records are the finest it emits.
            Self::Debug | Self::Trace => py_level::DEBUG,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LoggerConfig {
    level: LogLevel,
    access_log: bool,
    format: LogFormat,
}

impl LoggerConfig {
    fn new(level: LogLevel, access_log: bool, format: &str) -> Result<Self, LoggerError> {
        Ok(Self {
            level,
            access_log,
            format: format.parse()?,
        })
    }

    fn filter(&self) -> Result<EnvFilter, LoggerError> {
        let level = self.level.filter();
        let directives = if self.access_log {
            level.to_string()
        } else {
            format!("{level},pyronova::access=off")
        };
        EnvFilter::try_new(&directives).map_err(|source| LoggerError::Filter { directives, source })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LoggerError {
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
    /// The level the filter currently applies; every interpreter gates its Python logging
    /// on it.
    level: LogLevel,
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
/// - `level`: a `LogLevel`, or its name (see `LogLevel.parse`)
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
pub fn init_logger(level: &Bound<'_, PyAny>, access_log: bool, format: &str) -> PyResult<()> {
    let config = LoggerConfig::new(LogLevel::from_arg(level)?, access_log, format)?;
    apply(&config)?;
    tracing::info!(
        target: "pyronova::server",
        level = %config.level.filter(),
        access_log = config.access_log,
        format = ?config.format,
        "Pyronova tracing engine initialized"
    );
    Ok(())
}

/// The Python `logging` level matching the level `init_logger` applied, or `None` before
/// any `init_logger`. Every interpreter (main after `init_logger`, each worker in its
/// bootstrap) sets its root logger to it, so records below the threshold are dropped
/// before formatting or crossing into Rust. One source for the level: what `init_logger`
/// parsed, never re-read from the environment or re-mapped in Python.
#[pyfunction]
pub fn _python_log_level() -> Option<i64> {
    Some(LOGGER.lock().as_ref()?.level.python_level())
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
/// Called from `pyronova._log_bridge.RustLogHandler.emit()` in every interpreter with the
/// record's numeric `levelno`; `EnvFilter` does the filtering. `worker_id=None` (the main
/// interpreter) records no `worker` field at all, so a main-interpreter line can't pass
/// for worker 0's.
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
        assert_eq!("warning".parse::<LogLevel>().unwrap(), LogLevel::Warn);
        assert_eq!("Off".parse::<LogLevel>().unwrap(), LogLevel::Off);
        for bad in ["", "3", "verbose", "pyronova=debug"] {
            assert!(
                matches!(bad.parse::<LogLevel>(), Err(LoggerError::Level(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn python_level_lets_through_what_the_filter_logs() {
        assert_eq!(LogLevel::Off.python_level(), 60);
        assert_eq!(LogLevel::Error.python_level(), 40);
        assert_eq!(LogLevel::Warn.python_level(), 30);
        assert_eq!(LogLevel::Info.python_level(), 20);
        assert_eq!(LogLevel::Debug.python_level(), 10);
        assert_eq!(LogLevel::Trace.python_level(), 10);
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
        let config = LoggerConfig::new(LogLevel::Info, false, "text").unwrap();
        assert!(config
            .filter()
            .unwrap()
            .to_string()
            .contains("pyronova::access=off"));
    }
}
