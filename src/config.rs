//! The engine's server configuration, parsed once at the edge.
//!
//! `run()` reads every environment variable the engine honours here, once, into typed
//! values; nothing deeper in the run path reads the environment. A value that doesn't
//! parse is a startup error naming the variable and the raw text, never a silent
//! default. (`Pyronova.run()`'s own settings — host, port, workers, TLS — are resolved
//! from the environment on the Python side, in `_ServeSettings`, and passed in.)

use std::num::{NonZeroU64, NonZeroUsize};

use pyo3::prelude::*;

use crate::server::cpu::Cpus;

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
const BRIDGE_QUEUE_PER_WORKER: NonZeroUsize = NonZeroUsize::new(16).unwrap();

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
    #[cfg(target_os = "macos")]
    Fanout,
}

/// GC scheduling mode, parsed once at startup from `PYRONOVA_GC_MODE` (unset = count):
///   - `count` — each worker runs `gc.collect()` every `PYRONOVA_GC_THRESHOLD` requests it
///     serves (`SubInterpreterWorker::serve`). Predictable, can collide with bursty
///     traffic.
///   - `idle` — the TPC accept loop collects once a worker has run requests and then
///     none for a full `PYRONOVA_GC_IDLE_MS` tick (default 100ms), so the pause lands in
///     a lull. The worker's count trigger becomes the OOM failsafe at
///     `PYRONOVA_GC_OOM_FAILSAFE` requests (default 50_000), so sustained traffic can't
///     starve the collector. Needs the per-thread-listener TPC topology.
///   - `off` — no framework-level triggers at all. `gc.disable()` still runs at sub-interp
///     init; users must call `gc.collect()` themselves or accept ref-count-only cleanup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GcMode {
    Count,
    Idle,
    Off,
}

impl std::fmt::Display for GcMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            GcMode::Count => "count",
            GcMode::Idle => "idle",
            GcMode::Off => "off",
        })
    }
}

impl std::str::FromStr for GcMode {
    type Err = GcModeError;

    fn from_str(raw: &str) -> Result<Self, GcModeError> {
        match raw {
            "count" => Ok(GcMode::Count),
            "idle" => Ok(GcMode::Idle),
            "off" => Ok(GcMode::Off),
            _ => Err(GcModeError::Unknown(raw.to_string())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum GcModeError {
    #[error("{GC_MODE_ENV}={0:?} is not a GC mode; expected \"count\", \"idle\" or \"off\"")]
    Unknown(String),
    #[error("{GC_MODE_ENV}={mode} is not supported by {topology}")]
    Unsupported { mode: GcMode, topology: Topology },
}

/// The sizes `run()` was given; `None` takes the topology's default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Sizing {
    /// TPC threads, or the pool's sub-interpreters.
    pub(crate) workers: Option<NonZeroUsize>,
    /// Tokio threads of the multi-thread (`PYRONOVA_TPC=0`) server.
    pub(crate) io_workers: Option<NonZeroUsize>,
}

/// How one server serves, resolved once in `run()` from the mode, the environment and
/// the sizes. Every serving decision (accept loops, worker count, GC support, which
/// server runs) reads it; nothing re-derives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Topology {
    /// Thread-per-core; every handler on the main interpreter.
    TpcGil { threads: NonZeroUsize },
    /// Thread-per-core with one sub-interpreter per thread, each thread accepting on its
    /// own `SO_REUSEPORT` listeners.
    TpcWorkers { threads: NonZeroUsize },
    /// macOS opt-in (`PYRONOVA_TPC_DARWIN=fanout`): one acceptor thread fans connections
    /// out to the TPC worker threads.
    #[cfg(target_os = "macos")]
    TpcFanout { threads: NonZeroUsize },
    /// `PYRONOVA_TPC=0` in GIL mode: a multi-thread Tokio runtime.
    MultiThreadGil {
        io_threads: NonZeroUsize,
        accept_loops: NonZeroUsize,
    },
    /// `PYRONOVA_TPC=0` in sub-interpreter mode: a channel pool of workers behind a
    /// multi-thread Tokio runtime.
    Pool {
        workers: NonZeroUsize,
        io_threads: NonZeroUsize,
        accept_loops: NonZeroUsize,
    },
}

impl Topology {
    /// The topology `mode` and `env` select, sized from `sizing` or else `cpus`; a GC mode
    /// it can't run is an error here, before anything is bound or built.
    ///
    /// TPC defaults to one thread per PHYSICAL core: pinning two TPC threads to SMT
    /// siblings thrashes their shared L1 (measured -50% on a 7840HS), since every thread
    /// runs the same code path. The multi-thread server defaults to the logical count:
    /// work-stealing puts IO and bytecode on siblings, which have different footprints.
    pub(crate) fn resolve(
        mode: Mode,
        env: &EnvConfig,
        sizing: Sizing,
        cpus: Cpus,
    ) -> Result<Self, GcModeError> {
        let topology = if env.tpc {
            let threads = sizing.workers.unwrap_or(cpus.physical);
            match (mode, env.darwin_topology) {
                (Mode::Gil, _) => Topology::TpcGil { threads },
                (Mode::Subinterp, DarwinTopology::PerThreadListener) => {
                    Topology::TpcWorkers { threads }
                }
                #[cfg(target_os = "macos")]
                (Mode::Subinterp, DarwinTopology::Fanout) => Topology::TpcFanout { threads },
            }
        } else {
            let io_threads = sizing.io_workers.unwrap_or(cpus.logical);
            let accept_loops = multi_thread_accept_loops(io_threads, cpus.logical);
            match mode {
                Mode::Gil => Topology::MultiThreadGil {
                    io_threads,
                    accept_loops,
                },
                Mode::Subinterp => Topology::Pool {
                    workers: sizing.workers.unwrap_or(cpus.logical),
                    io_threads,
                    accept_loops,
                },
            }
        };
        if topology.supports(env.gc.mode) {
            Ok(topology)
        } else {
            Err(GcModeError::Unsupported {
                mode: env.gc.mode,
                topology,
            })
        }
    }

    /// One socket per listener for each of these: every TPC thread, the fanout acceptor,
    /// or the multi-thread runtime's accept tasks.
    pub(crate) fn accept_loops(self) -> NonZeroUsize {
        match self {
            Topology::TpcGil { threads } | Topology::TpcWorkers { threads } => threads,
            #[cfg(target_os = "macos")]
            Topology::TpcFanout { .. } => NonZeroUsize::MIN,
            Topology::MultiThreadGil { accept_loops, .. } | Topology::Pool { accept_loops, .. } => {
                accept_loops
            }
        }
    }

    /// Whether this topology can schedule `mode`'s collections. Only the per-thread TPC
    /// listener has the idle tick; the pool's workers count requests only. The GIL
    /// topologies run no workers, so every mode is moot, and accepted, there.
    pub(crate) fn supports(self, mode: GcMode) -> bool {
        match self {
            Topology::TpcGil { .. } | Topology::TpcWorkers { .. } => true,
            Topology::MultiThreadGil { .. } => true,
            #[cfg(target_os = "macos")]
            Topology::TpcFanout { .. } => matches!(mode, GcMode::Count | GcMode::Off),
            Topology::Pool { .. } => mode == GcMode::Count,
        }
    }
}

impl std::fmt::Display for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Topology::TpcGil { .. } => "the thread-per-core GIL server",
            Topology::TpcWorkers { .. } => "the thread-per-core sub-interpreter server",
            #[cfg(target_os = "macos")]
            Topology::TpcFanout { .. } => {
                "the Darwin fanout TPC topology (PYRONOVA_TPC_DARWIN=fanout)"
            }
            Topology::MultiThreadGil { .. } => "the multi-thread GIL server (PYRONOVA_TPC=0)",
            Topology::Pool { .. } => "the sub-interpreter pool (PYRONOVA_TPC=0)",
        })
    }
}

/// Accept loops of the multi-thread (`PYRONOVA_TPC=0`) server. Linux's `SO_REUSEPORT`
/// load-balances connections across several loops; macOS's doesn't, so it gets one.
#[cfg(target_os = "linux")]
fn multi_thread_accept_loops(io_threads: NonZeroUsize, logical: NonZeroUsize) -> NonZeroUsize {
    io_threads.min(logical)
}

#[cfg(not(target_os = "linux"))]
fn multi_thread_accept_loops(_io_threads: NonZeroUsize, _logical: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::MIN
}

/// Async-engine workers a TPC sub-interpreter server runs its `async def` routes on (the
/// TPC threads keep the `def` routes inline): one per TPC thread when no `def` route
/// needs the cores, half of them (at least one) when `def` routes share the cores.
pub(crate) fn tpc_async_workers(threads: NonZeroUsize, has_sync: bool) -> NonZeroUsize {
    if has_sync {
        NonZeroUsize::new(threads.get() / 2).unwrap_or(NonZeroUsize::MIN)
    } else {
        threads
    }
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
            #[cfg(target_os = "macos")]
            Some("fanout") => DarwinTopology::Fanout,
            #[cfg(not(target_os = "macos"))]
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
            None => workers.saturating_mul(BRIDGE_QUEUE_PER_WORKER),
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
        #[cfg(target_os = "macos")]
        assert_eq!(fanout.unwrap().darwin_topology, DarwinTopology::Fanout);
        #[cfg(not(target_os = "macos"))]
        assert!(fanout.is_err());
    }

    #[test]
    fn gc_mode_parses_the_three_modes_and_nothing_else() {
        assert_eq!("count".parse(), Ok(GcMode::Count));
        assert_eq!("idle".parse(), Ok(GcMode::Idle));
        assert_eq!("off".parse(), Ok(GcMode::Off));
        for raw in ["idel", "", "IDLE", " idle"] {
            assert_eq!(
                raw.parse::<GcMode>(),
                Err(GcModeError::Unknown(raw.to_string()))
            );
        }
        let message = "idel".parse::<GcMode>().unwrap_err().to_string();
        assert!(message.contains("PYRONOVA_GC_MODE") && message.contains("\"idel\""));
    }

    fn n(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    const CPUS: Cpus = Cpus {
        logical: NonZeroUsize::new(16).unwrap(),
        physical: NonZeroUsize::new(8).unwrap(),
    };

    fn resolve(
        mode: Mode,
        vars: &[(&'static str, &str)],
        sizing: Sizing,
    ) -> Result<Topology, GcModeError> {
        Topology::resolve(mode, &parse(vars).unwrap(), sizing, CPUS)
    }

    #[test]
    fn tpc_defaults_to_one_thread_per_physical_core() {
        let t = resolve(Mode::Subinterp, &[], Sizing::default()).unwrap();
        assert_eq!(t, Topology::TpcWorkers { threads: n(8) });
        assert_eq!(t.accept_loops(), n(8));
        let t = resolve(Mode::Gil, &[], Sizing::default()).unwrap();
        assert_eq!(t, Topology::TpcGil { threads: n(8) });
        let sized = Sizing {
            workers: Some(n(3)),
            io_workers: Some(n(99)),
        };
        let t = resolve(Mode::Subinterp, &[], sized).unwrap();
        assert_eq!(t, Topology::TpcWorkers { threads: n(3) });
        assert_eq!(t.accept_loops(), n(3));
    }

    #[test]
    fn tpc_off_sizes_from_the_logical_count() {
        let off = [("PYRONOVA_TPC", "0")];
        let t = resolve(Mode::Subinterp, &off, Sizing::default()).unwrap();
        let loops = if cfg!(target_os = "linux") { 16 } else { 1 };
        assert_eq!(
            t,
            Topology::Pool {
                workers: n(16),
                io_threads: n(16),
                accept_loops: n(loops),
            }
        );
        assert_eq!(t.accept_loops(), n(loops));
        let sized = Sizing {
            workers: Some(n(2)),
            io_workers: Some(n(4)),
        };
        let t = resolve(Mode::Gil, &off, sized).unwrap();
        let loops = if cfg!(target_os = "linux") { 4 } else { 1 };
        assert_eq!(
            t,
            Topology::MultiThreadGil {
                io_threads: n(4),
                accept_loops: n(loops),
            }
        );
    }

    #[test]
    fn the_pool_runs_count_mode_only() {
        let off = ("PYRONOVA_TPC", "0");
        assert!(resolve(
            Mode::Subinterp,
            &[off, ("PYRONOVA_GC_MODE", "count")],
            Sizing::default()
        )
        .is_ok());
        for mode in ["idle", "off"] {
            let err = resolve(
                Mode::Subinterp,
                &[off, ("PYRONOVA_GC_MODE", mode)],
                Sizing::default(),
            )
            .unwrap_err();
            assert!(
                matches!(
                    err,
                    GcModeError::Unsupported {
                        topology: Topology::Pool { .. },
                        ..
                    }
                ),
                "{err:?}"
            );
            let text = err.to_string();
            assert!(
                text.contains(mode) && text.contains("PYRONOVA_TPC=0"),
                "{text}"
            );
        }
        // GIL mode runs no workers: every GC mode is accepted.
        assert!(resolve(
            Mode::Gil,
            &[off, ("PYRONOVA_GC_MODE", "idle")],
            Sizing::default()
        )
        .is_ok());
    }

    #[test]
    fn per_thread_tpc_runs_every_gc_mode() {
        for mode in ["count", "idle", "off"] {
            assert!(resolve(
                Mode::Subinterp,
                &[("PYRONOVA_GC_MODE", mode)],
                Sizing::default()
            )
            .is_ok());
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn darwin_fanout_has_one_accept_loop_and_no_idle_mode() {
        let fanout = ("PYRONOVA_TPC_DARWIN", "fanout");
        let t = resolve(Mode::Subinterp, &[fanout], Sizing::default()).unwrap();
        assert_eq!(t, Topology::TpcFanout { threads: n(8) });
        assert_eq!(t.accept_loops(), NonZeroUsize::MIN);
        assert!(t.supports(GcMode::Off));
        let err = resolve(
            Mode::Subinterp,
            &[fanout, ("PYRONOVA_GC_MODE", "idle")],
            Sizing::default(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("idle") && err.to_string().contains("fanout"));
        // The fanout choice is a sub-interpreter topology; GIL mode stays per-thread.
        let t = resolve(Mode::Gil, &[fanout], Sizing::default()).unwrap();
        assert_eq!(t, Topology::TpcGil { threads: n(8) });
    }

    #[test]
    fn tpc_async_pool_shares_the_cores_with_sync_routes() {
        assert_eq!(tpc_async_workers(n(8), false), n(8));
        assert_eq!(tpc_async_workers(n(8), true), n(4));
        assert_eq!(tpc_async_workers(n(3), true), n(1));
        assert_eq!(tpc_async_workers(n(1), true), n(1));
        assert_eq!(tpc_async_workers(n(1), false), n(1));
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
