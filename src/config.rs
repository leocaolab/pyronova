//! The engine's server configuration, parsed once at the edge.
//!
//! `run()` reads every environment variable the engine honours here, once, into typed
//! values; nothing deeper in the run path reads the environment. A value that doesn't
//! parse is a startup error naming the variable and the raw text, never a silent
//! default. (`Pyronova.run()`'s own settings — host, port, workers, TLS — are resolved
//! from the environment on the Python side, in `_ServeSettings`, and passed in.)

use std::num::{NonZeroU64, NonZeroUsize};

use pyo3::prelude::*;

use crate::tpc::{GcMode, GcModeError};

/// Where non-`gil=True` handlers run.
#[pyclass(module = "pyronova.engine", eq, eq_int, frozen, hash)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Mode {
    /// Every handler on the main interpreter.
    Gil,
    /// Handlers in sub-interpreter workers; `gil=True` routes on the main interpreter.
    Subinterp,
}

#[pymethods]
impl Mode {
    /// The mode `name` spells: `"gil"` (or `"default"`), `"subinterp"` (or `"auto"`).
    /// Anything else raises `ValueError` naming the accepted spellings.
    #[staticmethod]
    fn parse(name: &str) -> PyResult<Self> {
        Ok(name.parse::<Mode>()?)
    }

    /// Whether this mode runs handlers in sub-interpreter workers.
    #[getter]
    fn uses_workers(&self) -> bool {
        *self == Mode::Subinterp
    }

    fn __str__(&self) -> &'static str {
        self.as_str()
    }
}

impl Mode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Mode::Gil => "gil",
            Mode::Subinterp => "subinterp",
        }
    }

    /// A `mode=` argument: a `Mode`, or its name.
    pub(crate) fn from_arg(arg: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(mode) = arg.cast::<Mode>() {
            return Ok(*mode.get());
        }
        let name: &str = arg.extract().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(format!(
                "mode must be a pyronova.engine.Mode or a str, got {}",
                arg.get_type()
                    .name()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|_| "an object".into())
            ))
        })?;
        Ok(name.parse::<Mode>()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("mode {0:?} is not a serving mode; expected \"gil\" (or \"default\") or \"subinterp\" (or \"auto\")")]
pub(crate) struct UnknownMode(pub(crate) String);

impl From<UnknownMode> for PyErr {
    fn from(e: UnknownMode) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}

impl std::str::FromStr for Mode {
    type Err = UnknownMode;

    fn from_str(name: &str) -> Result<Self, UnknownMode> {
        match name {
            "gil" | "default" => Ok(Mode::Gil),
            "subinterp" | "auto" => Ok(Mode::Subinterp),
            _ => Err(UnknownMode(name.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const TPC_ENV: &str = "PYRONOVA_TPC";
const TPC_DARWIN_ENV: &str = "PYRONOVA_TPC_DARWIN";
pub(crate) const GC_MODE_ENV: &str = "PYRONOVA_GC_MODE";
const GC_THRESHOLD_ENV: &str = "PYRONOVA_GC_THRESHOLD";
const GC_OOM_FAILSAFE_ENV: &str = "PYRONOVA_GC_OOM_FAILSAFE";
const GC_IDLE_MS_ENV: &str = "PYRONOVA_GC_IDLE_MS";
const BRIDGE_WORKERS_ENV: &str = "PYRONOVA_GIL_BRIDGE_WORKERS";
const BRIDGE_CAPACITY_ENV: &str = "PYRONOVA_GIL_BRIDGE_CAPACITY";
const METRICS_ENV: &str = "PYRONOVA_METRICS";

/// Requests a worker serves between scheduled `gc.collect()`s in count mode. Measured on
/// the baseline test: 5000 gave p99 = 2.0 ms, 100_000 gave p99 ≈ 300 µs, 0 (never) 240 µs.
const DEFAULT_GC_THRESHOLD: u64 = 100_000;
/// Idle mode's count trigger: collect after this many requests even without a lull.
const DEFAULT_GC_OOM_FAILSAFE: u64 = 50_000;
/// Idle mode's tick.
const DEFAULT_GC_IDLE_MS: NonZeroU64 = NonZeroU64::new(100).unwrap();
/// Main-interpreter bridge threads: handlers mix CPU and I/O, and an I/O-bound handler
/// releases the GIL for its peers.
const DEFAULT_BRIDGE_WORKERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
/// Bridge queue slots per bridge thread when the capacity isn't set.
const BRIDGE_QUEUE_PER_WORKER: usize = 16;

/// A variable that is set but doesn't parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("environment variable {var}={raw:?} is invalid: {expected}")]
pub(crate) struct EnvError {
    pub(crate) var: &'static str,
    pub(crate) raw: String,
    pub(crate) expected: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ConfigError {
    #[error(transparent)]
    Env(#[from] EnvError),
    #[error(transparent)]
    Gc(#[from] GcModeError),
}

impl From<ConfigError> for PyErr {
    fn from(e: ConfigError) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}

/// How the TPC sub-interpreter server takes connections on macOS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DarwinTopology {
    /// One `SO_REUSEPORT` listener per TPC thread (the default).
    PerThreadListener,
    /// One acceptor thread fans connections out to the TPC threads.
    Fanout,
}

/// GC scheduling for sub-interpreter workers (see `GcMode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GcConfig {
    pub(crate) mode: GcMode,
    /// Count mode: requests between collections; 0 never collects.
    pub(crate) threshold: u64,
    /// Idle mode: the count trigger that still fires without a lull.
    pub(crate) oom_failsafe: u64,
    /// Idle mode: how often the accept loop checks for a lull.
    pub(crate) idle_tick: std::time::Duration,
}

impl GcConfig {
    /// The request count at which a worker collects by itself (0 = never): the threshold
    /// in count mode, the OOM failsafe in idle mode, never in off mode.
    pub(crate) fn count_trigger(&self) -> u64 {
        match self.mode {
            GcMode::Count => self.threshold,
            GcMode::Idle => self.oom_failsafe,
            GcMode::Off => 0,
        }
    }
}

/// The main-interpreter bridge the TPC server runs `gil=True` routes on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BridgeConfig {
    pub(crate) workers: NonZeroUsize,
    pub(crate) capacity: NonZeroUsize,
}

/// Everything the engine takes from the environment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EnvConfig {
    /// `PYRONOVA_TPC=0` serves through the channel pool (and multi-thread Tokio for GIL
    /// mode) instead of thread-per-core.
    pub(crate) tpc: bool,
    pub(crate) darwin_topology: DarwinTopology,
    pub(crate) gc: GcConfig,
    pub(crate) bridge: BridgeConfig,
    /// `PYRONOVA_METRICS=1`: hot-path counters and the RSS sampler.
    pub(crate) metrics: bool,
}

impl EnvConfig {
    /// The process environment, parsed.
    pub(crate) fn from_env() -> Result<Self, ConfigError> {
        Self::parse(|var| {
            std::env::var_os(var).map(|raw| {
                raw.into_string()
                    .map_err(|raw| raw.to_string_lossy().into_owned())
            })
        })
    }

    /// `lookup(var)`: `None` when unset, `Err(lossy text)` when not UTF-8.
    pub(crate) fn parse(
        lookup: impl Fn(&'static str) -> Option<Result<String, String>>,
    ) -> Result<Self, ConfigError> {
        let get = |var: &'static str, expected: &'static str| -> Result<Option<String>, EnvError> {
            match lookup(var) {
                None => Ok(None),
                Some(Ok(raw)) => Ok(Some(raw)),
                Some(Err(raw)) => Err(EnvError { var, raw, expected }),
            }
        };
        let invalid = |var: &'static str, raw: String, expected: &'static str| EnvError {
            var,
            raw,
            expected,
        };

        const BOOL: &str = "expected 1/true/yes/on or 0/false/no/off";
        let tpc = match get(TPC_ENV, BOOL)? {
            None => true,
            Some(raw) => parse_bool(&raw).ok_or_else(|| invalid(TPC_ENV, raw, BOOL))?,
        };

        const TOPOLOGY: &str = "expected \"fanout\" or \"listener\"";
        let darwin_topology = match get(TPC_DARWIN_ENV, TOPOLOGY)?.as_deref() {
            None | Some("listener") => DarwinTopology::PerThreadListener,
            Some("fanout") if cfg!(target_os = "macos") => DarwinTopology::Fanout,
            Some("fanout") => {
                return Err(invalid(
                    TPC_DARWIN_ENV,
                    "fanout".into(),
                    "the fanout topology exists on macOS only",
                )
                .into())
            }
            Some(other) => return Err(invalid(TPC_DARWIN_ENV, other.into(), TOPOLOGY).into()),
        };

        let mode = match lookup(GC_MODE_ENV) {
            None => GcMode::Count,
            Some(Ok(raw)) => raw.parse()?,
            Some(Err(raw)) => return Err(GcModeError::Unknown(raw).into()),
        };
        const COUNT: &str = "expected a request count (0 or more)";
        let count = |var: &'static str, default: u64| -> Result<u64, EnvError> {
            match get(var, COUNT)? {
                None => Ok(default),
                Some(raw) => raw.parse().map_err(|_| invalid(var, raw, COUNT)),
            }
        };
        const MILLIS: &str = "expected a positive number of milliseconds";
        let idle_ms = match get(GC_IDLE_MS_ENV, MILLIS)? {
            None => DEFAULT_GC_IDLE_MS,
            Some(raw) => raw
                .parse::<NonZeroU64>()
                .map_err(|_| invalid(GC_IDLE_MS_ENV, raw, MILLIS))?,
        };
        let gc = GcConfig {
            mode,
            threshold: count(GC_THRESHOLD_ENV, DEFAULT_GC_THRESHOLD)?,
            oom_failsafe: count(GC_OOM_FAILSAFE_ENV, DEFAULT_GC_OOM_FAILSAFE)?,
            idle_tick: std::time::Duration::from_millis(idle_ms.get()),
        };

        const POSITIVE: &str = "expected a positive integer";
        let positive = |var: &'static str| -> Result<Option<NonZeroUsize>, EnvError> {
            get(var, POSITIVE)?
                .map(|raw| {
                    raw.parse::<NonZeroUsize>()
                        .map_err(|_| invalid(var, raw, POSITIVE))
                })
                .transpose()
        };
        let workers = positive(BRIDGE_WORKERS_ENV)?.unwrap_or(DEFAULT_BRIDGE_WORKERS);
        let capacity = match positive(BRIDGE_CAPACITY_ENV)? {
            Some(capacity) => capacity,
            None => workers.saturating_mul(
                NonZeroUsize::new(BRIDGE_QUEUE_PER_WORKER).expect("a positive constant"),
            ),
        };

        const FLAG: &str = "expected 1 or 0";
        let metrics = match get(METRICS_ENV, FLAG)?.as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            Some(other) => return Err(invalid(METRICS_ENV, other.into(), FLAG).into()),
        };

        Ok(EnvConfig {
            tpc,
            darwin_topology,
            gc,
            bridge: BridgeConfig { workers, capacity },
            metrics,
        })
    }
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(vars: &[(&'static str, &str)]) -> Result<EnvConfig, ConfigError> {
        let vars: Vec<(&'static str, String)> =
            vars.iter().map(|(k, v)| (*k, v.to_string())).collect();
        EnvConfig::parse(move |var| {
            vars.iter()
                .find(|(k, _)| *k == var)
                .map(|(_, v)| Ok(v.clone()))
        })
    }

    fn env_error(vars: &[(&'static str, &str)]) -> EnvError {
        match parse(vars) {
            Err(ConfigError::Env(e)) => e,
            other => panic!("expected an EnvError, got {other:?}"),
        }
    }

    #[test]
    fn nothing_set_is_the_documented_defaults() {
        let config = parse(&[]).unwrap();
        assert!(config.tpc);
        assert_eq!(config.darwin_topology, DarwinTopology::PerThreadListener);
        assert_eq!(
            config.gc,
            GcConfig {
                mode: GcMode::Count,
                threshold: 100_000,
                oom_failsafe: 50_000,
                idle_tick: std::time::Duration::from_millis(100),
            }
        );
        assert_eq!(config.bridge.workers.get(), 4);
        assert_eq!(config.bridge.capacity.get(), 64);
        assert!(!config.metrics);
    }

    #[test]
    fn mode_names_parse_and_a_typo_is_an_error() {
        assert_eq!("gil".parse(), Ok(Mode::Gil));
        assert_eq!("default".parse(), Ok(Mode::Gil));
        assert_eq!("subinterp".parse(), Ok(Mode::Subinterp));
        assert_eq!("auto".parse(), Ok(Mode::Subinterp));
        for typo in ["subinterpreter", "async", "GIL", "", "hybrid"] {
            assert_eq!(typo.parse::<Mode>(), Err(UnknownMode(typo.into())));
        }
        let text = "subinterpter".parse::<Mode>().unwrap_err().to_string();
        assert!(
            text.contains("\"subinterpter\"") && text.contains("\"gil\""),
            "{text}"
        );
    }

    #[test]
    fn tpc_is_a_boolean() {
        assert!(!parse(&[("PYRONOVA_TPC", "0")]).unwrap().tpc);
        assert!(!parse(&[("PYRONOVA_TPC", "off")]).unwrap().tpc);
        assert!(parse(&[("PYRONOVA_TPC", "1")]).unwrap().tpc);
        let e = env_error(&[("PYRONOVA_TPC", "maybe")]);
        assert_eq!((e.var, e.raw.as_str()), ("PYRONOVA_TPC", "maybe"));
    }

    #[test]
    fn gc_numbers_must_parse() {
        let config = parse(&[
            ("PYRONOVA_GC_THRESHOLD", "0"),
            ("PYRONOVA_GC_OOM_FAILSAFE", "7"),
            ("PYRONOVA_GC_IDLE_MS", "5"),
        ])
        .unwrap();
        assert_eq!(config.gc.threshold, 0);
        assert_eq!(config.gc.oom_failsafe, 7);
        assert_eq!(config.gc.idle_tick, std::time::Duration::from_millis(5));
        for (var, raw) in [
            ("PYRONOVA_GC_THRESHOLD", "5k"),
            ("PYRONOVA_GC_THRESHOLD", "-1"),
            ("PYRONOVA_GC_OOM_FAILSAFE", "lots"),
            ("PYRONOVA_GC_IDLE_MS", "0"),
            ("PYRONOVA_GC_IDLE_MS", "1.5"),
        ] {
            let e = env_error(&[(var, raw)]);
            assert_eq!((e.var, e.raw.as_str()), (var, raw));
        }
        assert!(matches!(
            parse(&[("PYRONOVA_GC_MODE", "idel")]),
            Err(ConfigError::Gc(GcModeError::Unknown(raw))) if raw == "idel"
        ));
    }

    #[test]
    fn count_trigger_follows_the_gc_mode() {
        let mut gc = parse(&[
            ("PYRONOVA_GC_THRESHOLD", "10"),
            ("PYRONOVA_GC_OOM_FAILSAFE", "20"),
        ])
        .unwrap()
        .gc;
        assert_eq!(gc.count_trigger(), 10);
        gc.mode = GcMode::Idle;
        assert_eq!(gc.count_trigger(), 20);
        gc.mode = GcMode::Off;
        assert_eq!(gc.count_trigger(), 0);
    }

    #[test]
    fn bridge_sizes_are_positive() {
        let config = parse(&[("PYRONOVA_GIL_BRIDGE_WORKERS", "2")]).unwrap();
        assert_eq!(config.bridge.capacity.get(), 32);
        let config = parse(&[("PYRONOVA_GIL_BRIDGE_CAPACITY", "5")]).unwrap();
        assert_eq!(config.bridge.capacity.get(), 5);
        for (var, raw) in [
            ("PYRONOVA_GIL_BRIDGE_CAPACITY", "0"),
            ("PYRONOVA_GIL_BRIDGE_CAPACITY", "big"),
            ("PYRONOVA_GIL_BRIDGE_WORKERS", "0"),
            ("PYRONOVA_GIL_BRIDGE_WORKERS", "four"),
        ] {
            let e = env_error(&[(var, raw)]);
            assert_eq!((e.var, e.raw.as_str()), (var, raw));
        }
    }

    #[test]
    fn metrics_and_topology_take_their_values_only() {
        assert!(parse(&[("PYRONOVA_METRICS", "1")]).unwrap().metrics);
        assert_eq!(
            env_error(&[("PYRONOVA_METRICS", "yes")]).var,
            "PYRONOVA_METRICS"
        );
        assert_eq!(
            env_error(&[("PYRONOVA_TPC_DARWIN", "fan-out")]).var,
            "PYRONOVA_TPC_DARWIN"
        );
        let fanout = parse(&[("PYRONOVA_TPC_DARWIN", "fanout")]);
        if cfg!(target_os = "macos") {
            assert_eq!(fanout.unwrap().darwin_topology, DarwinTopology::Fanout);
        } else {
            assert!(fanout.is_err());
        }
    }

    #[test]
    fn a_non_utf8_value_is_an_error_not_unset() {
        let result = EnvConfig::parse(|var| {
            (var == "PYRONOVA_GIL_BRIDGE_WORKERS").then(|| Err("\u{fffd}".to_string()))
        });
        assert!(matches!(
            result,
            Err(ConfigError::Env(EnvError {
                var: "PYRONOVA_GIL_BRIDGE_WORKERS",
                ..
            }))
        ));
    }
}
