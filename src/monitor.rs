//! Passive GIL contention monitor + decoupled RSS sampler.
//!
//! Previous design used an active watchdog thread that acquired the GIL every
//! 10ms to probe contention. This caused two problems:
//!
//! 1. **Observer effect**: the probe itself competes for the GIL, creating
//!    artificial contention and context switches (~5-10% throughput loss under
//!    heavy Python workloads).
//! 2. **Shutdown segfault**: the detached watchdog thread could outlive
//!    Py_Finalize, causing use-after-free on the global interpreter state.
//!
//! New design (Haskell bracket-inspired):
//! - **GIL metrics** are collected passively on the real request path — each
//!   `call_handler_with_hooks` records GIL acquisition wait time as a
//!   byproduct. Zero overhead when idle, zero artificial contention.
//! - **RSS sampling** runs in a separate non-GIL thread while a server with metrics
//!   serves ([`RssSampling`]); the last one to stop joins it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crossbeam_utils::CachePadded;
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// GIL contention metrics (updated passively by request handlers)
// ---------------------------------------------------------------------------

/// Last GIL acquisition wait time (microseconds)
pub static GIL_LATENCY_LAST_US: AtomicU64 = AtomicU64::new(0);
/// Peak GIL acquisition wait time since last reset (microseconds)
pub static GIL_LATENCY_MAX_US: AtomicU64 = AtomicU64::new(0);
/// Total probe count (= total handler invocations that acquired GIL)
pub static GIL_PROBE_COUNT: AtomicU64 = AtomicU64::new(0);
/// Total accumulated GIL wait (microseconds)
pub static GIL_TOTAL_WAIT_US: AtomicU64 = AtomicU64::new(0);

/// Process RSS in bytes from the background sampler's latest read. `None` until the
/// sampler has read it once (it runs only while a server with `PYRONOVA_METRICS=1`
/// serves), after a read that failed, and once the last such server stopped.
static MEMORY_RSS_BYTES: parking_lot::Mutex<Option<u64>> = parking_lot::Mutex::new(None);

/// Number of threads currently waiting to acquire the main GIL
pub static GIL_QUEUE_LENGTH: std::sync::atomic::AtomicIsize =
    std::sync::atomic::AtomicIsize::new(0);
/// Peak business handler GIL hold time (microseconds, reset on read)
pub static GIL_HOLD_MAX_US: AtomicU64 = AtomicU64::new(0);

// Hot-path counters: CachePadded to avoid false sharing across CPU cores.
// Each counter gets its own 64-byte cache line.

/// Requests dropped due to backpressure (503 overloaded)
pub static DROPPED_REQUESTS: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
/// Total requests processed
pub static TOTAL_REQUESTS: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));

/// Master kill-switch for hot-path metrics. Read once at startup from
/// `PYRONOVA_METRICS` (same env var that gates the RSS sampler). When
/// false, per-request `fetch_add` on TOTAL_REQUESTS is skipped — a
/// cache-line that was being ping-ponged across every core on every
/// request goes cold, reclaiming the last cross-core atomic in the
/// TPC inline hot path. Users running in production with metrics
/// dashboards flip PYRONOVA_METRICS=1 and pay the ~30ns/req back.
static METRICS_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Set the metrics kill-switch (`PYRONOVA_METRICS`, parsed by `config::EnvConfig`).
/// Called at every `run()` startup. Idempotent.
pub fn init_metrics_flag(on: bool) {
    METRICS_ENABLED.store(on, std::sync::atomic::Ordering::Relaxed);
}

/// Whether hot-path metrics (TOTAL_REQUESTS etc.) should be recorded.
/// Branch-predicted false in the default path — a no-op after the
/// first iteration of the JIT trace.
#[inline(always)]
pub fn metrics_enabled() -> bool {
    METRICS_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Increment TOTAL_REQUESTS iff metrics are enabled. Preferred over
/// `TOTAL_REQUESTS.fetch_add` at hot-path call sites — the default-off
/// branch completely skips the cross-core atomic.
#[inline(always)]
pub fn count_request() {
    if metrics_enabled() {
        TOTAL_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Passive GIL measurement (called from handlers.rs)
// ---------------------------------------------------------------------------

/// A GIL wait longer than this is logged as congestion.
const GIL_CONGESTED_WARN_US: u64 = 50_000;

/// Record a GIL acquisition wait time. Called from `call_handler_with_hooks`
/// immediately after `Python::attach` succeeds.
///
/// This replaces the active watchdog probe — measures real request latency
/// instead of artificial contention from a background thread.
#[inline]
pub fn record_gil_wait(wait_us: u64) {
    GIL_LATENCY_LAST_US.store(wait_us, Ordering::Relaxed);
    GIL_LATENCY_MAX_US.fetch_max(wait_us, Ordering::Relaxed);
    GIL_TOTAL_WAIT_US.fetch_add(wait_us, Ordering::Relaxed);
    GIL_PROBE_COUNT.fetch_add(1, Ordering::Relaxed);

    if wait_us > GIL_CONGESTED_WARN_US {
        tracing::warn!(
            target: "pyronova::server",
            latency_ms = wait_us / 1000,
            "GIL congested (measured on real request)"
        );
    }
}

// ---------------------------------------------------------------------------
// Decoupled RSS sampler (no GIL, deterministic shutdown)
// ---------------------------------------------------------------------------

/// The sampler thread, shared by every server serving with metrics: the first
/// [`RssSampling::start`] spawns it, the last one's drop stops and joins it.
struct Sampler {
    users: usize,
    /// Dropping the sender stops the thread at once (it waits on the receiver between
    /// samples); `None` while no thread runs, or if spawning it failed.
    thread: Option<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
}

static SAMPLER: parking_lot::Mutex<Sampler> = parking_lot::Mutex::new(Sampler {
    users: 0,
    thread: None,
});

/// RSS sampling for as long as it is held: one per serving run with `PYRONOVA_METRICS=1`.
/// Every stop path of the run drops it (return, error, `shutdown()`, SIGINT); when the last
/// holder goes, the thread is joined (it must not outlive `Py_Finalize`, which may unload
/// this library under it) and `rss_bytes` reads `None` again, not a stale value.
pub(crate) struct RssSampling(());

impl RssSampling {
    pub(crate) fn start() -> Self {
        let mut sampler = SAMPLER.lock();
        sampler.users += 1;
        if sampler.users == 1 {
            let (stop_tx, stop_rx) = std::sync::mpsc::channel();
            match std::thread::Builder::new()
                .name("pyronova-rss-sampler".to_string())
                .spawn(move || sample_rss_until_stopped(stop_rx))
            {
                Ok(handle) => sampler.thread = Some((stop_tx, handle)),
                // A passive observability feature: serve without it rather than fail.
                Err(e) => tracing::warn!(
                    target: "pyronova::server",
                    error = %e,
                    "failed to spawn RSS sampler; continuing without RSS metrics"
                ),
            }
        }
        RssSampling(())
    }
}

impl Drop for RssSampling {
    fn drop(&mut self) {
        // Held through the join (immediate: the thread wakes on the disconnect), so a
        // server starting meanwhile spawns its thread after this one's last write.
        let mut sampler = SAMPLER.lock();
        sampler.users -= 1;
        if sampler.users > 0 {
            return;
        }
        if let Some((stop_tx, handle)) = sampler.thread.take() {
            drop(stop_tx);
            if let Err(panic) = handle.join() {
                tracing::error!(target: "pyronova::server", ?panic, "RSS sampler thread panicked");
            }
        }
        *MEMORY_RSS_BYTES.lock() = None;
    }
}

/// RSS doesn't change fast enough to warrant more frequent sampling, and the sampler
/// does no GIL work.
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

fn sample_rss_until_stopped(stop: std::sync::mpsc::Receiver<()>) {
    // Warn on the first failed read after a good one (or at start), not every interval.
    let mut warn_on_failure = true;
    loop {
        let rss = match get_rss_bytes() {
            Ok(bytes) => {
                warn_on_failure = true;
                Some(bytes)
            }
            Err(e) => {
                if warn_on_failure {
                    tracing::warn!(target: "pyronova::server", error = %e,
                        "RSS read failed; metrics report rss_bytes=None until a read succeeds");
                }
                warn_on_failure = false;
                None
            }
        };
        *MEMORY_RSS_BYTES.lock() = rss;
        // Only a disconnect (the last holder dropped) ends the wait early.
        if stop.recv_timeout(RSS_SAMPLE_INTERVAL) != Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        {
            break;
        }
    }
    tracing::debug!(target: "pyronova::server", "RSS sampler stopped");
}

/// Return the OS page size in bytes at runtime.
///
/// `/proc/self/statm` reports RSS in pages; the page size is 4 KiB on x86_64
/// but 16 KiB or 64 KiB on aarch64 Linux.
#[cfg(target_os = "linux")]
fn page_size_bytes() -> std::io::Result<u64> {
    // SAFETY: sysconf(_SC_PAGESIZE) takes no pointers; it returns -1 on failure.
    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(ps).map_err(|_| std::io::Error::last_os_error())
}

/// Current process RSS in bytes (platform-specific, zero dependencies).
fn get_rss_bytes() -> std::io::Result<u64> {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: an all-zero bit pattern is a valid value of this plain-integer struct.
        let mut info: libc_mach_task_basic_info = unsafe { std::mem::zeroed() };
        let mut count = MACH_TASK_BASIC_INFO_COUNT;
        // SAFETY: `info` is a writable MACH_TASK_BASIC_INFO buffer and `count` holds its
        // length in natural_t words, as task_info requires.
        let kr = unsafe { mach_task_self_info(&mut info, &mut count) };
        if kr != KERN_SUCCESS {
            return Err(std::io::Error::other(format!(
                "task_info(MACH_TASK_BASIC_INFO) returned kern_return_t {kr}"
            )));
        }
        Ok(info.resident_size)
    }
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm")?;
        let pages = statm
            .split_whitespace()
            .nth(1)
            .and_then(|field| field.parse::<u64>().ok())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected /proc/self/statm contents {statm:?}"),
                )
            })?;
        Ok(pages * page_size_bytes()?)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "RSS sampling is implemented for Linux and macOS only",
        ))
    }
}

// macOS: minimal FFI for task_info (avoids libc crate dependency)
#[cfg(target_os = "macos")]
#[repr(C)]
struct libc_mach_task_basic_info {
    virtual_size: u64,
    resident_size: u64,
    resident_size_max: u64,
    user_time: [u32; 2],
    system_time: [u32; 2],
    policy: i32,
    suspend_count: i32,
}

/// `<mach/task_info.h>` flavor for `mach_task_basic_info`.
#[cfg(target_os = "macos")]
const MACH_TASK_BASIC_INFO: u32 = 20;
/// Buffer length in `natural_t` (u32) words, as `task_info` counts it.
#[cfg(target_os = "macos")]
const MACH_TASK_BASIC_INFO_COUNT: u32 =
    (std::mem::size_of::<libc_mach_task_basic_info>() / std::mem::size_of::<u32>()) as u32;
#[cfg(target_os = "macos")]
const KERN_SUCCESS: i32 = 0;

#[cfg(target_os = "macos")]
unsafe fn mach_task_self_info(info: &mut libc_mach_task_basic_info, count: &mut u32) -> i32 {
    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(task: u32, flavor: u32, info: *mut u8, count: *mut u32) -> i32;
    }
    task_info(
        mach_task_self(),
        MACH_TASK_BASIC_INFO,
        info as *mut _ as *mut u8,
        count,
    )
}

// ---------------------------------------------------------------------------
// Python-facing metrics API
// ---------------------------------------------------------------------------

/// A snapshot of the engine's counters. Reading one has no side effect; peaks are
/// cleared only by `reset_peaks()`.
#[pyclass(frozen, get_all, name = "Metrics", module = "pyronova.engine")]
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Latest GIL acquisition wait (µs).
    gil_wait_last_us: u64,
    /// Longest GIL acquisition wait since the last `reset_peaks()` (µs).
    gil_wait_peak_us: u64,
    /// Handler invocations whose GIL wait was measured.
    gil_wait_count: u64,
    /// Sum of all measured GIL waits (µs).
    gil_wait_total_us: u64,
    /// Threads currently waiting for the main GIL.
    gil_queue_length: isize,
    /// Longest handler GIL hold since the last `reset_peaks()` (µs).
    gil_hold_peak_us: u64,
    /// Process RSS from the sampler (bytes); `None` if it has no successful read.
    rss_bytes: Option<u64>,
    /// Requests refused with 503 because the server was overloaded.
    dropped_requests: u64,
    /// Requests counted; `None` while hot-path metrics are off (`PYRONOVA_METRICS` unset).
    total_requests: Option<u64>,
}

#[pymethods]
impl Metrics {
    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

/// Read every counter. Does not reset anything.
#[pyfunction]
pub fn get_gil_metrics() -> Metrics {
    Metrics {
        gil_wait_last_us: GIL_LATENCY_LAST_US.load(Ordering::Relaxed),
        gil_wait_peak_us: GIL_LATENCY_MAX_US.load(Ordering::Relaxed),
        gil_wait_count: GIL_PROBE_COUNT.load(Ordering::Relaxed),
        gil_wait_total_us: GIL_TOTAL_WAIT_US.load(Ordering::Relaxed),
        gil_queue_length: GIL_QUEUE_LENGTH.load(Ordering::Relaxed),
        gil_hold_peak_us: GIL_HOLD_MAX_US.load(Ordering::Relaxed),
        rss_bytes: *MEMORY_RSS_BYTES.lock(),
        dropped_requests: DROPPED_REQUESTS.load(Ordering::Relaxed),
        total_requests: metrics_enabled().then(|| TOTAL_REQUESTS.load(Ordering::Relaxed)),
    }
}

/// Clear the GIL wait and hold peaks, starting a new peak window.
#[pyfunction]
pub fn reset_peaks() {
    GIL_LATENCY_MAX_US.store(0, Ordering::Relaxed);
    GIL_HOLD_MAX_US.store(0, Ordering::Relaxed);
}
