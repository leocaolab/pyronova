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

/// Pins the calling thread to `core_id`, if given. Pinning is an optimization: where the
/// OS refuses (a restricted container, macOS) the thread runs unpinned, logged at debug.
pub(crate) fn try_pin_current(core_id: Option<core_affinity::CoreId>) {
    if let Some(core) = core_id {
        if !core_affinity::set_for_current(core) {
            tracing::debug!(target: "pyronova::server", core = core.id, "could not pin the thread to its core");
        }
    }
}

/// macOS: raises the calling thread's QoS class to `USER_INTERACTIVE`, which keeps it on
/// the performance cores. Darwin has no CPU pinning, and a TPC thread parked on an
/// efficiency core has no peer to steal its work.
#[cfg(target_os = "macos")]
pub(crate) fn elevate_thread_qos_macos() {
    use std::os::raw::c_int;
    // `QOS_CLASS_USER_INTERACTIVE` in <sys/qos.h>.
    const QOS_CLASS_USER_INTERACTIVE: c_int = 0x21;
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: c_int, relative_priority: c_int) -> c_int;
    }
    // SAFETY: sets the calling thread's own QoS class; no pointers.
    let rc = unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
    if rc != 0 {
        tracing::warn!(
            target: "pyronova::server",
            rc,
            "pthread_set_qos_class_self_np failed; TPC thread may be \
             scheduled on E-cores — expect throughput collapse"
        );
    }
}

#[cfg(not(target_os = "macos"))]
#[inline(always)]
pub(crate) fn elevate_thread_qos_macos() {}

/// Physical CPU cores, which size the TPC pool; the logical count where they can't be
/// read.
///
/// Linux: the distinct `thread_siblings_list`s under /sys/devices/system/cpu (SMT
/// siblings count once). macOS: `hw.perflevel0.physicalcpu`, the performance cores only:
/// a TPC thread on an efficiency core serves about a third as fast, and nothing steals its
/// work.
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
