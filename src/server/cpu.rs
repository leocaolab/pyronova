//! The host's CPUs: how many there are, and keeping a serving thread on a fast core.

use std::num::NonZeroUsize;

/// The CPU counts a server is sized from, read once per run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Cpus {
    /// Logical CPUs (SMT siblings counted).
    pub(crate) logical: NonZeroUsize,
    /// Physical cores (see [`physical_core_count`]).
    pub(crate) physical: NonZeroUsize,
}

impl Cpus {
    pub(crate) fn detect() -> Self {
        Cpus {
            logical: logical_cpu_count(),
            physical: physical_core_count(),
        }
    }
}

/// Logical CPUs the process may use; one if the OS can't say.
fn logical_cpu_count() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN)
}

/// Pin the current OS thread to a specific CPU core if one is
/// available. Silently no-ops on platforms where `core_affinity`
/// can't enumerate (e.g. restricted containers with no CPU mask
/// visibility); in that case the OS scheduler still gets us
/// statistically-close-to-core-local execution on the per-thread
/// runtime because the runtime never migrates tasks, only the
/// kernel can move the thread.
pub(crate) fn try_pin_current(core_id: Option<core_affinity::CoreId>) {
    if let Some(c) = core_id {
        let _ = core_affinity::set_for_current(c);
    }
}

/// macOS-only: bump the calling thread's QoS class to
/// USER_INTERACTIVE. core_affinity::set_for_current is a silent
/// no-op on Darwin (no public CPU-pinning API), so without this
/// the scheduler is free to park TPC threads on E-cores for
/// power savings — fatal under TPC because there is no work-
/// stealing across threads. USER_INTERACTIVE tells the scheduler
/// to keep us on P-cores and ignore power hints, at the cost of
/// giving up energy-efficiency on idle machines. Acceptable
/// tradeoff for a throughput-first server.
#[cfg(target_os = "macos")]
pub(crate) fn elevate_thread_qos_macos() {
    use std::os::raw::c_int;
    // Opaque qos_class_t. 0x21 == QOS_CLASS_USER_INTERACTIVE per
    // <sys/qos.h>. Keeping the constant inline avoids pulling in
    // the whole qos.h shim; the value has been stable since 10.10.
    const QOS_CLASS_USER_INTERACTIVE: c_int = 0x21;
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: c_int, relative_priority: c_int) -> c_int;
    }
    unsafe {
        let rc = pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        // A failure here can park TPC threads on E-cores; log it so the throughput drop
        // has a visible cause.
        if rc != 0 {
            tracing::warn!(
                target: "pyronova::server",
                rc,
                "pthread_set_qos_class_self_np failed; TPC thread may be \
                 scheduled on E-cores — expect throughput collapse"
            );
        }
    }
}

#[cfg(not(target_os = "macos"))]
#[inline(always)]
pub(crate) fn elevate_thread_qos_macos() {}

/// Count physical CPU cores to size the TPC pool.
///
/// Linux: parses /sys/devices/system/cpu/cpu*/topology/thread_siblings_list —
/// the number of unique sibling groups equals the physical core count,
/// stripping SMT.
///
/// macOS: queries `hw.perflevel0.physicalcpu` via sysctl. On Apple
/// Silicon perflevel0 is the performance-core cluster; the efficiency
/// cores at perflevel1 are deliberately excluded. Running a TPC
/// thread on an E-core tanks single-connection throughput to ~1/3,
/// and with no work-stealing that request is stuck — so the whole
/// tail latency collapses. Sizing to P-core count keeps every TPC
/// thread on a fast cluster.
///
/// Other platforms: falls back to logical core count.
#[cfg(target_os = "linux")]
pub(crate) fn physical_core_count() -> NonZeroUsize {
    use std::collections::HashSet;
    use std::fs;

    let Ok(entries) = fs::read_dir("/sys/devices/system/cpu") else {
        return logical_cpu_count();
    };

    let mut sibling_groups: HashSet<String> = HashSet::new();
    for e in entries.flatten() {
        let name = e.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("cpu") || !name[3..].chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let path = e.path().join("topology/thread_siblings_list");
        if let Ok(s) = fs::read_to_string(&path) {
            sibling_groups.insert(s.trim().to_string());
        }
    }
    NonZeroUsize::new(sibling_groups.len()).unwrap_or_else(logical_cpu_count)
}

#[cfg(target_os = "macos")]
pub(crate) fn physical_core_count() -> NonZeroUsize {
    let name = c"hw.perflevel0.physicalcpu";
    let mut count: i32 = 0;
    let mut size = std::mem::size_of::<i32>();
    // SAFETY: `name` is NUL-terminated; `count`/`size` describe a writable i32.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut count as *mut _ as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    match usize::try_from(count).ok().and_then(NonZeroUsize::new) {
        Some(n) if rc == 0 => n,
        // Pre-Apple-Silicon macOS (no perf levels) or older kernels: the logical count.
        _ => logical_cpu_count(),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn physical_core_count() -> NonZeroUsize {
    logical_cpu_count()
}
