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
//! - **RSS sampling** runs in a separate non-GIL thread with an explicit
//!   stop flag and JoinHandle for deterministic shutdown.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// Memory RSS in bytes (updated by background sampler)
pub static MEMORY_RSS_BYTES: AtomicU64 = AtomicU64::new(0);

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

/// Initialize the metrics kill-switch. Called once at app.run() startup.
/// Idempotent.
pub fn init_metrics_flag() {
    let on = std::env::var("PYRONOVA_METRICS").unwrap_or_default() == "1";
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

    if wait_us > 50_000 {
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

/// Stop flag for the RSS sampler thread.
static RSS_SAMPLER_RUNNING: AtomicBool = AtomicBool::new(false);

/// Handle to the spawned sampler thread. `stop_rss_sampler` takes it
/// and joins — the previous code dropped the handle immediately,
/// leaving the thread racing `Py_Finalize` during process exit. If
/// the extension's `.so` was unloaded before the sampler's sleep(1)
/// woke up, the thread's next instruction pointed at freed pages →
/// segfault (spurious non-zero exit signalled K8s / systemd etc.).
static RSS_SAMPLER_HANDLE: std::sync::Mutex<Option<std::thread::JoinHandle<()>>> =
    std::sync::Mutex::new(None);

/// Spawn a lightweight background thread that samples process RSS.
/// The handle is stashed in `RSS_SAMPLER_HANDLE`; `stop_rss_sampler`
/// joins it on shutdown.
///
/// This thread never touches Python or the GIL — it only reads /proc/self/statm.
pub fn spawn_rss_sampler() {
    // Hold the handle lock across the whole check-and-spawn so two callers
    // can't race, and bail out if a sampler is already installed. Spawning a
    // second would overwrite (and thus detach) the first's JoinHandle, leaving
    // stop_rss_sampler able to join only the newest — the detached thread would
    // then outlive Py_Finalize and segfault on freed code pages (ISSUE-72).
    let mut slot = RSS_SAMPLER_HANDLE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if slot.is_some() {
        return;
    }
    RSS_SAMPLER_RUNNING.store(true, Ordering::Release);
    let handle = match std::thread::Builder::new()
        .name("pyronova-rss-sampler".to_string())
        .spawn(|| {
            while RSS_SAMPLER_RUNNING.load(Ordering::Acquire) {
                MEMORY_RSS_BYTES.store(get_rss_bytes(), Ordering::Relaxed);
                // 1 second interval — RSS doesn't change fast enough to warrant
                // more frequent sampling, and this thread does zero GIL work.
                std::thread::sleep(Duration::from_secs(1));
            }
            tracing::debug!(target: "pyronova::server", "RSS sampler stopped");
        }) {
        Ok(handle) => handle,
        Err(e) => {
            // RSS sampling is a passive observability feature. If the OS
            // refuses the thread (resource exhaustion, ulimit), don't take
            // the whole server down — degrade gracefully and serve requests
            // without RSS metrics. Reset the run flag so stop_rss_sampler is
            // a no-op and a later spawn attempt starts clean.
            RSS_SAMPLER_RUNNING.store(false, Ordering::Release);
            tracing::warn!(
                target: "pyronova::server",
                error = %e,
                "failed to spawn RSS sampler; continuing without RSS metrics"
            );
            return;
        }
    };
    // slot was locked at the top and confirmed empty above, so this never
    // overwrites (and thus detaches) a live handle.
    *slot = Some(handle);
}

/// Signal the RSS sampler to stop AND join the thread. Blocks up to
/// one sample interval (~1s) while the thread wakes from its sleep,
/// observes the stop flag, and exits. On process shutdown this is
/// mandatory — otherwise `Py_Finalize` can unload the Rust extension
/// while the sampler thread is still sleeping, and waking into freed
/// code pages segfaults.
pub fn stop_rss_sampler() {
    RSS_SAMPLER_RUNNING.store(false, Ordering::Release);
    // Same poison-recovery rationale as the install side (arc monitor-1).
    let handle = RSS_SAMPLER_HANDLE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    if let Some(h) = handle {
        if let Err(panic) = h.join() {
            tracing::error!(target: "pyronova::server", ?panic,
                "RSS sampler thread panicked");
        }
    }
}

/// Return the OS page size in bytes at runtime.
///
/// `/proc/self/statm` reports RSS in pages; the page size is 4 KiB on x86_64
/// but 16 KiB or 64 KiB on aarch64 Linux. Using a hardcoded 4096 causes
/// under-reporting by 4x–16x on those hosts.
#[cfg(target_os = "linux")]
fn page_size_bytes() -> u64 {
    // SAFETY: sysconf with _SC_PAGESIZE is always well-defined, never fails.
    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if ps > 0 {
        ps as u64
    } else {
        4096
    }
}

/// Get current process RSS in bytes (platform-specific, zero dependencies).
fn get_rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        use std::mem;
        unsafe {
            let mut info: libc_mach_task_basic_info = mem::zeroed();
            let mut count = (mem::size_of::<libc_mach_task_basic_info>() / 4) as u32;
            let kr = mach_task_self_info(&mut info, &mut count);
            if kr == 0 {
                return info.resident_size;
            }
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
            .map(|pages| pages * page_size_bytes())
            .unwrap_or(0)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
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

#[cfg(target_os = "macos")]
unsafe fn mach_task_self_info(info: &mut libc_mach_task_basic_info, count: &mut u32) -> i32 {
    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(task: u32, flavor: u32, info: *mut u8, count: *mut u32) -> i32;
    }
    // MACH_TASK_BASIC_INFO = 20
    task_info(mach_task_self(), 20, info as *mut _ as *mut u8, count)
}

// ---------------------------------------------------------------------------
// Python-facing metrics API
// ---------------------------------------------------------------------------

/// Get all metrics. Returns tuple:
/// (last_us, peak_us, probe_count, total_wait_us, rss_bytes,
///  queue_len, hold_peak_us, dropped_requests, total_requests)
/// Resets peaks after read.
#[pyfunction]
pub fn get_gil_metrics() -> (u64, u64, u64, u64, u64, isize, u64, u64, u64) {
    let last = GIL_LATENCY_LAST_US.load(Ordering::Relaxed);
    let peak = GIL_LATENCY_MAX_US.swap(0, Ordering::Relaxed);
    let count = GIL_PROBE_COUNT.load(Ordering::Relaxed);
    let total = GIL_TOTAL_WAIT_US.load(Ordering::Relaxed);
    let rss = MEMORY_RSS_BYTES.load(Ordering::Relaxed);
    let queue = GIL_QUEUE_LENGTH.load(std::sync::atomic::Ordering::Relaxed);
    let hold_peak = GIL_HOLD_MAX_US.swap(0, Ordering::Relaxed);
    let dropped = DROPPED_REQUESTS.load(Ordering::Relaxed);
    let total_req = TOTAL_REQUESTS.load(Ordering::Relaxed);
    (
        last, peak, count, total, rss, queue, hold_peak, dropped, total_req,
    )
}
