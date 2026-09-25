"""Bootstrap run in each sub-interpreter worker before the user's script.

A worker runs the same program as the main interpreter: the user's script
imports the real `pyronova` package and its engine. This bootstrap only
prepares the interpreter for it (Layer 2, FR-10):

- routes Python `logging` through Rust `tracing` (`pyronova.engine.emit_python_log`),
  tagged with this worker's id;
- hands cycle collection to the Rust engine's schedule (`gc.disable()`);
- installs per-worker C-extension isolation (private library copies for
  extensions that can't be shared between interpreters).

It runs as the module `__pyronova_bootstrap__`, in its own namespace. Rust sets
`WORKER_ID` and `POOL_ID` in it before it runs.
"""

# -- Python logging bridge to Rust tracing -----------------------------------

import logging as _logging
import os as _os
import sys, os

# Set once `pyronova.engine` is imported, at the end of this file (after the
# isolation machinery is installed, so the package's own imports go through it).
_emit_python_log = None


class _PyronovaRustHandler(_logging.Handler):
    """Routes Python logging records through Rust tracing, tagged with this
    worker's id. Records logged before the engine is imported (during this
    bootstrap) go to stderr."""

    def __init__(self, worker_id):
        super().__init__()
        self._worker_id = worker_id

    def emit(self, record):
        try:
            msg = record.getMessage()
            # Preserve exception tracebacks (logger.exception / exc_info=True).
            # Use a local variable rather than mutating record.exc_text so the
            # same LogRecord can safely be routed to multiple handlers.
            if record.exc_info:
                exc_text = record.exc_text or self.formatException(record.exc_info)
                msg = f"{msg}\n{exc_text}"
            if _emit_python_log is None:
                sys.stderr.write(f"{record.levelname} {record.name}: {msg}\n")
                return
            _emit_python_log(
                record.levelno,
                record.name,
                msg,
                record.pathname or "",
                record.lineno or 0,
                self._worker_id,
            )
        except Exception:
            # Never crash business logic due to logging. `handleError` is
            # Python's own "I tried to log and it blew up" hook — it
            # respects `logging.raiseExceptions` (False in production) and
            # writes a diagnostic to sys.stderr with the failing record,
            # which `pass` silently discarded. Upstream handlers on every
            # stdlib logging class use this exact pattern.
            self.handleError(record)

_root = _logging.getLogger()
_root.handlers.clear()
_root.addHandler(_PyronovaRustHandler(WORKER_ID))
# Sync Python's level gate with Rust's EnvFilter — rejects calls below
# threshold *before* getMessage() formatting or FFI crossing occurs.
# e.g. level=ERROR → logger.debug() returns immediately, no FFI overhead.
_PYRONOVA_LEVEL_MAP = {
    "TRACE": _logging.DEBUG, "DEBUG": _logging.DEBUG,
    "INFO": _logging.INFO, "WARN": _logging.WARNING, "WARNING": _logging.WARNING,
    "ERROR": _logging.ERROR, "CRITICAL": _logging.CRITICAL,
    "OFF": _logging.CRITICAL + 10,
}
_log_level_str = _os.environ.get("PYRONOVA_LOG_LEVEL", "DEBUG").upper()
if _log_level_str not in _PYRONOVA_LEVEL_MAP:
    print(
        f"pyronova: unrecognized PYRONOVA_LOG_LEVEL={_log_level_str!r}, defaulting to DEBUG",
        file=sys.stderr,
    )
_root.setLevel(_PYRONOVA_LEVEL_MAP.get(_log_level_str, _logging.DEBUG))

# -- Smart GC: hand Python GC scheduling off to the Rust engine --------------
#
# CPython's default GC triggers on a per-generation allocation threshold
# (gc.get_threshold() = (700, 10, 10) by default). At 400k+ rps that
# threshold is tripped HUNDREDS of times per second, each hit blocking
# the current thread for generation-0 scan + possibly escalating to gen-1
# or gen-2. On the request hot path this translates into P99 tail
# latency spikes of 10-50ms even on an otherwise well-behaved workload.
#
# Fix: turn off CPython's automatic trigger entirely. The Rust engine
# holds a cached `gc.collect` function pointer per sub-interp and fires
# it at a configurable request-count interval (default 5000, control
# via `PYRONOVA_GC_THRESHOLD=N` — set 0 to disable scheduled collection
# entirely on workloads that never accrete cycles).
#
# Ref counting still runs on every DECREF to zero, so non-cyclic garbage
# is collected instantly. Only cycle-collection waits for the timer.
# For the standard Pyronova request path (where Request + Response are
# ref-counted to zero by tp_dealloc at the end of each handler), there
# are effectively no cycles to collect — gc.collect() becomes a
# zero-cost safety valve.
try:
    import gc as _gc
    _gc.disable()
except Exception:
    # NEVER silently swallow. If gc.disable() fails, automatic GC stays
    # on — at 400k RPS that means hundreds of generation-0 collections
    # per second, each adding 10-50ms to P99 tail latency. Without this
    # log the regression is invisible until production monitoring catches
    # the tail spike. Emit at ERROR so it shows up in default log filters.
    _logging.getLogger("pyronova.bootstrap").error(
        "gc.disable() failed — CPython auto-GC remains active; "
        "expect P99 tail spikes at high RPS",
        exc_info=True,
    )


# ---------------------------------------------------------------------------
# Per-worker C-extension isolation.
# Clone a library into THIS sub-interpreter's own path so process-global-state
# extensions (numpy, orjson, ...) run isolated instead of colliding across
# sub-interpreters ("does not support loading in subinterpreters").
#
# Two ways in, one machinery:
#   • PROACTIVE — `app.isolate("numpy")` records the lib in PYRONOVA_ISOLATE_LIBS;
#     `_pyronova_isolate_libs()` clones it at worker init, before the user script.
#   • REACTIVE  — `_iso_import` (installed as builtins.__import__ below) catches the
#     "does not support loading in subinterpreters" / PyO3 #576 ImportError, reads
#     the offending binary module straight out of the error, isolates it, and
#     retries — so `import numpy` just works with no `app.isolate(...)` at all.
# Both share the helpers below and one per-worker clone dir (`_ISO["worker_dir"]`).
# ---------------------------------------------------------------------------

# Per-sub-interpreter isolation state. `worker_dir` is the private clone dir
# (claimed once, reused across proactive + reactive isolations); `isolated` is
# the set of top-level package names already cloned into it.
_ISO = {"worker_dir": None, "path_inserted": False, "isolated": set()}
# Above this many cloned bytes, an auto-isolated lib is flagged (heavy per-worker
# copy). Not an error — a visibility guard so a silent 215 MB × N-worker clone
# never happens. Tune / silence-warn via env.
_ISO_WARN_BYTES = int(_os.environ.get("PYRONOVA_ISOLATE_WARN_BYTES", str(100 * 1024 * 1024)))


def _iso_transient_override(value=-1):
    """Set THIS interpreter's multi-interp extension check override (-1: allow
    extensions that don't support sub-interpreters; 1: enforce the check),
    returning (previous_value, ok). Pair with `_iso_restore_override` in a
    try/finally.

    The override MUST be transient. `_override_multi_interp_extensions_check` is a
    per-interpreter GLOBAL switch: leaving it on lets the NEXT un-isolated
    single-phase extension load SHARED (un-isolated) instead of hard-failing —
    and that hard failure is exactly the signal reactive auto-isolate relies on.
    (Measured: after isolating orjson with a persistent override, numpy then
    loaded shared and was never isolated.) So flip it on only around a clone's
    import and restore it right after. ok=False on the main interpreter (the call
    raises there — no per-worker copy is needed anyway)."""
    import _imp
    try:
        return _imp._override_multi_interp_extensions_check(value), True
    except RuntimeError:
        return None, False


def _iso_restore_override(prev):
    if prev is not None:
        import _imp
        _imp._override_multi_interp_extensions_check(prev)


def _iso_resolve_src(lib):
    """On-disk source dir (package) or file (single-file ext) for `lib` in the
    ORIGINAL install, or None.

    Searches sys.path without the isolate root. Once one lib is isolated, this
    worker's clone dir is on sys.path, and it can already hold a clone of the
    NEXT lib (clone dirs are reused across runs). Resolving to that clone made
    `_iso_clone_lib` compare the clone against itself, find the signature
    stale, rmtree it and copy from the path it had just deleted; the retry then
    loaded the shared site-packages file (sklearn after scipy: "Interpreter
    change detected - this module can only be loaded into one interpreter per
    process").

    Uses PathFinder (searches the given path directly, ignoring sys.modules)
    rather than importlib.util.find_spec: a single-phase extension re-init can
    leave the module in sys.modules with __spec__=None (seen with pydantic_core's
    internal _pydantic_core.so), and find_spec() raises ValueError on such an
    entry."""
    import sys, importlib.util, importlib.machinery
    root = _os.path.realpath(_os.environ.get("PYRONOVA_ISOLATE_DIR", "/tmp/pyronova-isolate"))
    path = [p for p in sys.path
            if not (_os.path.realpath(p or ".") + _os.sep).startswith(root + _os.sep)]
    try:
        spec = importlib.machinery.PathFinder.find_spec(lib, path)
    except (ValueError, ImportError, AttributeError):
        spec = None
    if spec is None:
        # Not on a plain path entry (e.g. an editable install's own finder).
        try:
            spec = importlib.util.find_spec(lib)
        except (ValueError, ImportError, AttributeError):
            spec = None
        if spec is not None and (_os.path.realpath(spec.origin or "") + _os.sep).startswith(root + _os.sep):
            spec = None  # that's a clone, not the original
    if spec is None:
        return None
    if spec.submodule_search_locations:
        return list(spec.submodule_search_locations)[0]
    if spec.origin and spec.origin not in ("built-in", "frozen"):
        return spec.origin
    return None


def _iso_worker_dir(seed_libs):
    """Claim (once) this worker's private clone dir and add it to sys.path.

    Idempotent — later calls return the same dir regardless of `seed_libs`, so a
    reactive isolation reuses the dir a proactive one already claimed.

    Bucket layout: declared libs get a bucket keyed by their (path+mtime+size)
    signature, so an unchanged set reuses the SAME clones (same inodes) across
    restarts — on macOS the kernel verifies a `.dylib`'s code signature on first
    dlopen per inode (a numpy+scipy+sklearn set is hundreds of libs → seconds
    per worker), and a reused inode is verified once ever. A pure-reactive worker
    (nothing declared) uses a stable shared `auto` bucket; per-worker isolation
    still comes from the `w{idx}` slot below, and per-lib freshness from the
    `.sig` manifest in `_iso_clone_lib`."""
    if _ISO["worker_dir"] is not None:
        return _ISO["worker_dir"]
    import os, sys, fcntl, hashlib
    root = os.environ.get("PYRONOVA_ISOLATE_DIR", "/tmp/pyronova-isolate")
    if seed_libs:
        sig = hashlib.sha1()
        for lib in seed_libs:
            src = _iso_resolve_src(lib)
            if src is None:
                continue
            st = os.stat(src)
            sig.update(("%s\0%d\0%d\0" % (src, st.st_mtime_ns, st.st_size)).encode())
        bucket = sig.hexdigest()[:16]
    else:
        bucket = "auto"
    base = os.path.join(root, bucket)
    os.makedirs(base, exist_ok=True)
    # Claim a worker slot: grab the first free `w{i}.lock` (non-blocking) and
    # hold it for the process lifetime (the fd is deliberately left open —
    # closing it would release the slot). Concurrent servers therefore get
    # distinct slots, while a fresh run of a single server reuses the low
    # indices, i.e. the previous run's clone dirs (same, already-verified
    # inodes). Held per open-file-description, so sibling workers in this same
    # process also each get a distinct slot.
    idx = 0
    while True:
        lk = os.open(os.path.join(base, "w%d.lock" % idx), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(lk, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError:
            os.close(lk)
            idx += 1
    worker_dir = os.path.join(base, "w%d" % idx)
    os.makedirs(worker_dir, exist_ok=True)
    _ISO["worker_dir"] = worker_dir
    # NOTE: do NOT put worker_dir on sys.path here. The clone SOURCE must always
    # resolve to the ORIGINAL install (site-packages), never to a prior clone —
    # if worker_dir were on the path before cloning, on a warm restart
    # `_iso_resolve_src` would find the existing clone and treat it as the source,
    # and the freshness check would rmtree-then-cp it onto itself, destroying it.
    # Path insertion happens in `_iso_ensure_on_path`, AFTER cloning.
    return worker_dir


def _iso_ensure_on_path(worker_dir):
    """Put this worker's clone dir at the front of sys.path (once), so imports of
    isolated libs resolve to the private copy. Called AFTER cloning — see the
    ordering note in `_iso_worker_dir`."""
    import sys
    if not _ISO["path_inserted"]:
        sys.path.insert(0, worker_dir)
        _ISO["path_inserted"] = True


def _iso_clone_lib(lib, worker_dir, pkg2dist):
    """Clone `lib` (and its vendored `.libs`) into `worker_dir`. Returns the
    cloned destination path (dir or file), or None if the lib can't be resolved.

    Freshness: a per-lib `<basename>.sig` manifest records the source signature.
    An existing clone is reused only if the manifest still matches — a lib upgrade
    (mtime/size change) forces a re-clone. This covers reactively-added libs the
    bucket signature can't see, and makes declared-lib upgrades safe regardless of
    the bucket keying."""
    import os, subprocess, shutil, platform
    src = _iso_resolve_src(lib)
    if src is None:
        return None
    st = os.stat(src)
    cur_sig = "%d\0%d" % (st.st_mtime_ns, st.st_size)
    clone = ["cp", "-c", "-R"] if platform.system() == "Darwin" else ["cp", "--reflink=auto", "-R"]

    def _clone(s, d, sig_path=None):
        if os.path.exists(d):
            if sig_path is None:
                return  # vendored .libs: no manifest, reuse as-is
            try:
                with open(sig_path) as f:
                    if f.read() == cur_sig:
                        return  # up-to-date clone, inodes already verified
            except OSError:
                pass
            shutil.rmtree(d, ignore_errors=True)  # stale (lib upgraded) — re-clone
        # Clone into a temp dir and atomically rename it into place, so a
        # first-ever concurrent boot never observes a half-written copy.
        tmp = "%s.tmp-%d" % (d, os.getpid())
        subprocess.run(clone + [s, tmp], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            os.replace(tmp, d)
        except OSError:
            shutil.rmtree(tmp, ignore_errors=True)  # lost the race — use theirs
        if sig_path is not None:
            try:
                with open(sig_path, "w") as f:
                    f.write(cur_sig)
            except OSError:
                pass

    base_name = os.path.basename(src)
    dst = os.path.join(worker_dir, base_name)
    _clone(src, dst, os.path.join(worker_dir, base_name + ".sig"))
    # Also clone the wheel's vendored shared libs: on Linux these live in a
    # sibling `<name>.libs/` dir (e.g. numpy.libs/ holds OpenBLAS, referenced
    # by RPATH next to the package); macOS bundles them inside `<pkg>/.dylibs/`.
    # `<name>` is the DISTRIBUTION name, which can differ from the import name
    # — e.g. `sklearn` (import) ships its libgomp in `scikit_learn.libs/`, so
    # guessing `<import_name>.libs` alone misses it. Clone every spelling.
    parent = os.path.dirname(src)
    libs_names = {base_name}
    for dist in pkg2dist.get(base_name, []):
        libs_names.add(dist.replace("-", "_"))
    for ln in libs_names:
        vendored = os.path.join(parent, ln + ".libs")
        if os.path.isdir(vendored):
            _clone(vendored, os.path.join(worker_dir, os.path.basename(vendored)))
    return dst


# pyronova itself is never cloned, evicted or re-executed (Layer 2, FR-11): its engine
# keeps process-wide state (the main-interpreter handle, the async worker registry, the
# logger, the Postgres pool) that must exist once, and re-executing the package in a
# worker would duplicate `pyronova.context.ctx` and its ContextVars.
_ISO_PYRONOVA_REFUSED = (
    "pyronova cannot be isolated: one shared copy of pyronova and its engine is "
    "required (it keeps process-wide state), so it is never cloned, evicted or "
    "re-executed in a worker. Remove it from app.isolate(...)."
)


def _iso_is_pyronova(name):
    return name == "pyronova" or name.startswith("pyronova.")


def _iso_evict(lib):
    """Drop `lib` and its submodules from sys.modules so the next `import`
    resolves to THIS worker's fresh clone, not a cached (possibly stub) entry.
    A single-phase extension can leave a __spec__=None stub for its internal .so
    (e.g. pydantic_core -> _pydantic_core), which would otherwise shadow the clone
    and surface as "cannot import name ... (unknown location)"."""
    import sys
    if _iso_is_pyronova(lib):
        raise RuntimeError(_ISO_PYRONOVA_REFUSED)
    for name in list(sys.modules):
        if name == lib or name.startswith(lib + "."):
            del sys.modules[name]


def _iso_pkg2dist():
    import importlib.metadata as _md
    try:
        return _md.packages_distributions()
    except Exception:
        return {}


def _iso_report(lib, dst):
    """Never-silent cost guard: log the auto-isolated lib and its cloned size,
    warning past `_ISO_WARN_BYTES`. Cloning a heavy .so (polars is 215 MB) into
    every worker is real per-worker memory once it's dlopen'd — surface it."""
    import os
    total = 0
    if os.path.isdir(dst):
        for r, _d, files in os.walk(dst):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(r, f))
                except OSError:
                    pass
    else:
        try:
            total = os.path.getsize(dst)
        except OSError:
            pass
    mb = total / (1024 * 1024)
    log = _logging.getLogger("pyronova.isolate")
    if total >= _ISO_WARN_BYTES:
        log.warning(
            "auto-isolate: cloned %r (%.1f MB) into this worker — heavy per-worker "
            "copy; declare it in app.isolate(%r) to make the cost explicit",
            lib, mb, lib,
        )
    else:
        log.info(
            "auto-isolate: cloned %r (%.1f MB) into this worker "
            "(import tripped sub-interpreter isolation)",
            lib, mb,
        )


def _iso_isolate(mod_name):
    """Clone the top-level package of `mod_name` into this worker and record it,
    so the caller's retry (under a transient override) loads the private copy.
    Returns True if the package is isolated (freshly cloned OR already staged),
    False if its source can't be resolved. Cloning happens once per package per
    worker; a freshly-cloned lib is reported (cost guard), an already-staged one
    is not (proactive already logged the declaration)."""
    lib = mod_name.split(".")[0]  # clone the top-level package, not the inner .so
    if lib == "pyronova":
        raise RuntimeError(_ISO_PYRONOVA_REFUSED)
    if lib in _ISO["isolated"]:
        return True  # already staged; caller still retries under transient override
    worker_dir = _iso_worker_dir(seed_libs=())
    dst = _iso_clone_lib(lib, worker_dir, _iso_pkg2dist())  # resolves source pre-path-insert
    if dst is None:
        return False
    _iso_ensure_on_path(worker_dir)  # now the clone can shadow the original
    _ISO["isolated"].add(lib)
    _iso_evict(lib)
    _iso_report(lib, dst)
    return True


def _pyronova_isolate_libs():
    """PROACTIVE path: pre-stage a per-worker clone of every lib declared via
    `app.isolate()` at worker init, before the user script runs, and put the
    worker's clone dir on sys.path. The user's `import numpy` then resolves to
    the private clone, whose C extension loads first-try under the meta_path
    finder's transient override (below) — no failed attempt, warm-restart safe.
    Proactive just does the cloning up front so the first request pays no `cp`.
    (No persistent override: that would mask the collision signal an UNDECLARED
    single-phase lib needs to trigger its own reactive clone.)"""
    import os
    libs = [x.strip() for x in os.environ.get("PYRONOVA_ISOLATE_LIBS", "").split(",") if x.strip()]
    if not libs:
        return
    if "pyronova" in libs:
        raise RuntimeError(_ISO_PYRONOVA_REFUSED)
    # Sub-interpreter probe: the override raises on the main interp, where no
    # per-worker copy is needed. Probe and restore — real loads flip it later.
    prev, ok = _iso_transient_override()
    if not ok:
        return
    _iso_restore_override(prev)
    resolvable = [lib for lib in libs if _iso_resolve_src(lib) is not None]
    if not resolvable:
        return
    worker_dir = _iso_worker_dir(seed_libs=resolvable)
    pkg2dist = _iso_pkg2dist()
    cloned_any = False
    for lib in resolvable:
        # Clone while worker_dir is NOT yet on sys.path, so each source resolves
        # to the original install, not a sibling clone (warm-restart safety).
        if _iso_clone_lib(lib, worker_dir, pkg2dist) is not None:
            _ISO["isolated"].add(lib)
            cloned_any = True
    if cloned_any:
        _iso_ensure_on_path(worker_dir)
        for lib in resolvable:
            _iso_evict(lib)


# -- Load-time override: a meta_path finder for C extensions -----------------
#
# An extension that doesn't support per-interpreter GILs refuses to load in a
# worker unless the per-interpreter override is set. A meta_path finder swaps the
# real ExtensionFileLoader for one that sets the override only for this worker's
# private copies, and builds those in the worker without CPython's PROCESS-GLOBAL
# extension table (`_PyRuntime.imports.extensions`) — the table `sys.modules`
# eviction can't clear, whose pollution by a failed-then-retried load of the same
# clone made a warm restart abort with "cannot load module more than once per
# process". A load that fails is always of a shared file, and its retry is of a
# clone: a different file, so a different key. Declared libs (pre-staged clones
# already on sys.path) load isolated on the first try.

import builtins as _builtins
import importlib.machinery as _machinery
_iso_real_import = _builtins.__import__
_iso_in_hook = False  # re-entrancy guard: only the OUTERMOST import self-heals


def _iso_is_private_clone(path):
    """True if `path` is inside this worker's private clone dir."""
    wd = _ISO["worker_dir"]
    if not wd or not path:
        return False
    wd = _os.path.realpath(wd)
    return _os.path.realpath(path).startswith(wd + _os.sep)


def _iso_dynamic_strtab(path):
    """The string table of `path`'s dynamic symbols (ELF `.dynstr`, Mach-O
    `LC_SYMTAB` strings), or None if the file isn't a binary this can read.

    Only the string table: the names of the symbols the binary imports are all
    in it, and it is a small slice of a file that can be hundreds of MB.

    Imports nothing: it runs while an extension is being loaded, which can be in
    the middle of importing `struct` (for its `_struct`)."""
    def le(b):
        return int.from_bytes(b, "little")

    def be(b):
        return int.from_bytes(b, "big")

    with open(path, "rb") as f:
        head = f.read(64)
        base = 0
        if head[:4] == b"\xca\xfe\xba\xbe":  # universal Mach-O: pick this arch's slice
            want = {"arm64": 0x0100000C, "x86_64": 0x01000007}.get(_os.uname().machine)
            f.seek(8)
            for _ in range(be(head[4:8])):
                arch = f.read(20)  # cputype, cpusubtype, offset, size, align
                if be(arch[0:4]) == want:
                    base = be(arch[8:12])
                    break
            else:
                return None
            f.seek(base)
            head = f.read(64)
        if head[:4] == b"\xcf\xfa\xed\xfe":  # Mach-O 64, little-endian
            f.seek(base + 32)
            for _ in range(le(head[16:20])):  # ncmds
                cmd = f.read(8)
                body = f.read(le(cmd[4:8]) - 8)
                if le(cmd[0:4]) == 0x2:  # LC_SYMTAB: symoff, nsyms, stroff, strsize
                    f.seek(base + le(body[8:12]))
                    return f.read(le(body[12:16]))
            return None
        if head[:4] == b"\x7fELF" and head[4] == 2 and head[5] == 1:  # ELF64 LE
            shoff, shentsize, shnum = le(head[0x28:0x30]), le(head[0x3A:0x3C]), le(head[0x3C:0x3E])
            if not shoff or not shnum:
                return None
            f.seek(shoff)
            sections = []  # (sh_type, sh_offset, sh_size, sh_link)
            for _ in range(shnum):
                sh = f.read(shentsize)
                sections.append((le(sh[4:8]), le(sh[24:32]), le(sh[32:40]), le(sh[40:44])))
            for sh_type, _off, _size, link in sections:
                if sh_type == 11:  # SHT_DYNSYM; sh_link = its string table
                    _t, off, size, _l = sections[link]
                    f.seek(off)
                    return f.read(size)
            return None
    return None


def _iso_is_single_phase_file(path):
    """True if the extension at `path` is (or may be) single-phase init, decided
    WITHOUT running its init.

    A single-phase `PyInit_*` builds its module with `PyModule_Create2` (what the
    `PyModule_Create` macro calls); a multi-phase one only returns its def
    through `PyModuleDef_Init`. So a binary that imports `PyModule_Create2` is
    treated as single-phase. Checked against the runtime answer (the def's
    `m_slots`) for every extension module numpy, scipy, sklearn, orjson,
    pydantic_core, msgpack and isojson load (203 on macOS/Mach-O, 186 on
    Linux/ELF): all 9 single-phase ones flagged, no multi-phase one flagged. A binary this can't read counts as single-phase:
    the cost of a wrong yes is one extra per-worker copy."""
    try:
        strtab = _iso_dynamic_strtab(path)
    except (OSError, ValueError, IndexError):
        strtab = None
    if strtab is None:
        return True
    return b"PyModule_Create2\x00" in strtab


def _iso_create_here(spec):
    """Create an extension module entirely in THIS interpreter: call its
    `PyInit_*` here, and for a multi-phase one build the module from the
    returned def here. None if the binary has no `PyInit_<name>` symbol.

    CPython 3.13+ runs every extension's `PyInit_*` in the MAIN interpreter
    (import.c `import_run_extension` -> `switch_to_main_interpreter`):
    - single-phase: the module is built there and the worker gets a shallow
      copy of its dict, so its objects live on main's obmalloc heap and the
      first one the worker mutates frees main's memory into its own allocator
      (scipy's f2py `_fblas`: setattr on a fortran object -> `free(): invalid
      size`);
    - multi-phase: `PyInit_*` should only return the def, but many also call
      `import_array()` there (scipy's `_arpacklib`), which then imports numpy in
      main and stores MAIN's numpy C-API table in the binary's statics.

    Only for a private clone: nothing else loads that file, so skipping
    CPython's process-wide extension registry can't double-initialize its C
    statics. Exec slots run later through the loader's `exec_module`."""
    import sys
    ctypes = _iso_ctypes
    short = spec.name.rpartition(".")[2]
    lib = ctypes.PyDLL(spec.origin, mode=sys.getdlopenflags())
    try:
        init = getattr(lib, "PyInit_" + short)
    except AttributeError:
        return None
    # Take the result as a raw pointer. A multi-phase PyInit returns its static
    # PyModuleDef; wrapping that as an owned object (restype=py_object) decrefs it
    # when dropped. A C extension's def is immortal, so nothing happens, but PyO3's
    # starts at refcount 1: the decref frees static memory -> abort ("pointer being
    # freed was not allocated", measured with polars' _polars_runtime on 3.14).
    init.restype = ctypes.c_void_p
    ptr = init()
    if not ptr:
        raise SystemError(f"PyInit_{short} returned NULL without an exception")
    api = ctypes.pythonapi
    api.PyObject_Type.restype = ctypes.c_void_p      # new ref to the TYPE; the object is untouched
    api.PyObject_Type.argtypes = [ctypes.c_void_p]
    ob_type = api.PyObject_Type(ptr)
    api.Py_DecRef(ctypes.c_void_p(ob_type))
    if ob_type == ctypes.addressof(ctypes.c_char.in_dll(api, "PyModuleDef_Type")):
        # multi-phase: CPython's own next step, here instead of after its switch
        # to main (it also enforces Py_mod_multiple_interpreters under the
        # current override). The def stays borrowed, as CPython keeps it.
        api.PyModule_FromDefAndSpec2.restype = ctypes.py_object
        api.PyModule_FromDefAndSpec2.argtypes = [ctypes.c_void_p, ctypes.py_object, ctypes.c_int]
        return api.PyModule_FromDefAndSpec2(ptr, spec, sys.api_version)
    obj = ctypes.cast(ptr, ctypes.py_object).value  # takes its own reference
    api.Py_DecRef(ctypes.c_void_p(ptr))             # release the one PyInit returned
    api.PyModule_GetDef.restype = ctypes.c_void_p
    api.PyModule_GetDef.argtypes = [ctypes.py_object]
    api.PyState_AddModule.argtypes = [ctypes.py_object, ctypes.c_void_p]
    moddef = api.PyModule_GetDef(obj)
    # What CPython's fixup does: register it for PyState_FindModule.
    if moddef and api.PyState_AddModule(obj, moddef) != 0:
        raise ImportError(f"PyState_AddModule failed for {spec.name}")
    obj.__file__ = spec.origin
    return obj


class _IsolatingExtensionLoader:
    """Wraps an ExtensionFileLoader. Decides, per file, how a worker may load it:

    - a file in this worker's private clone dir: under the override, built
      entirely in this worker (`_iso_create_here`);
    - a shared file (site-packages, stdlib): with CPython's own
      `Py_mod_multiple_interpreters` check enforced, so an extension that
      doesn't support per-interpreter GILs fails with an isolation error and
      `_iso_import` clones its package. A single-phase one is refused before its
      init runs (CPython would run that init in the main interpreter).

    Any ImportError leaving the loader carries the module's name, so
    `_iso_import` knows which package to clone even when a package re-raises it
    as its own error."""

    def __init__(self, inner):
        self._inner = inner

    def create_module(self, spec):
        try:
            if _iso_is_private_clone(spec.origin):
                prev, _ok = _iso_transient_override()
                try:
                    mod = _iso_create_here(spec)
                    return mod if mod is not None else self._inner.create_module(spec)
                finally:
                    _iso_restore_override(prev)
            prev, ok = _iso_transient_override(1)  # enforce the check
            try:
                if ok and _iso_is_single_phase_file(spec.origin):
                    raise _iso_shared_single_phase_error(spec)
                return self._inner.create_module(spec)
            finally:
                _iso_restore_override(prev)
        except ImportError as exc:
            if exc.name is None:
                exc.name = spec.name
            raise

    def exec_module(self, module):
        clone = _iso_is_private_clone(getattr(module, "__file__", None))
        prev, _ok = _iso_transient_override(-1 if clone else 1)
        try:
            self._inner.exec_module(module)
        except ImportError as exc:
            if exc.name is None:
                exc.name = module.__name__
            raise
        finally:
            _iso_restore_override(prev)

    def __getattr__(self, attr):
        # get_filename / is_package / get_code / get_source / etc.
        return getattr(self._inner, attr)


class _SharedBuiltinLoader:
    """A built-in single-phase module (faulthandler, pulled in by
    sklearn -> joblib -> loky) has no file to clone, so it can only load
    shared, under the override. Known hazard: CPython runs its init in the main
    interpreter, so its objects are main's."""

    def __init__(self, inner):
        self._inner = inner

    def create_module(self, spec):
        prev, _ok = _iso_transient_override()
        try:
            return self._inner.create_module(spec)
        finally:
            _iso_restore_override(prev)

    def exec_module(self, module):
        prev, _ok = _iso_transient_override()
        try:
            self._inner.exec_module(module)
        finally:
            _iso_restore_override(prev)

    def __getattr__(self, attr):
        return getattr(self._inner, attr)


class _IsolatingExtensionFinder:
    """meta_path finder that, for C-extension and built-in imports ONLY, swaps
    in the loaders above. Everything else returns None so the default finders
    handle it. Uses PathFinder / BuiltinImporter (which don't consult
    meta_path) to resolve the real spec, so there's no recursion."""

    def find_spec(self, fullname, path, target=None):
        if _iso_is_pyronova(fullname):
            # The engine declares per-interpreter support and loads through CPython's
            # own check; the loaders here never touch it (FR-11).
            return None
        if path is None:
            spec = _machinery.BuiltinImporter.find_spec(fullname)
            if spec is not None:
                spec.loader = _SharedBuiltinLoader(spec.loader)
                return spec
        try:
            spec = _machinery.PathFinder.find_spec(fullname, path, target)
        except (ImportError, AttributeError, ValueError):
            return None
        if spec is None or not isinstance(spec.loader, _machinery.ExtensionFileLoader):
            return None  # not a C extension — let the default finders handle it
        spec.loader = _IsolatingExtensionLoader(spec.loader)
        return spec


# -- REACTIVE self-heal: clone an undeclared lib on the load that collides
#
# A shared extension file that doesn't support per-interpreter GILs fails to
# load in a worker (CPython's own check, enforced by the loader above; or the
# lib's own guard: numpy "cannot load module more than once per process",
# Cython "can only be loaded into one interpreter per process"). That is the
# signal that the lib needs a per-worker copy. We catch it at the top-level
# import, clone the package the failing module belongs to, and restart the
# import, which then resolves to the private clone (a DIFFERENT file).

_ISO_SHARED_SINGLE_PHASE = "single-phase extension loaded from a shared file"


def _iso_shared_single_phase_error(spec):
    return ImportError(
        f"{spec.name}: {_ISO_SHARED_SINGLE_PHASE} ({spec.origin}). CPython 3.13+ "
        "runs a single-phase init in the main interpreter and gives this worker "
        "objects owned by main's heap, so it must load from this worker's private "
        f"copy; pyronova clones {spec.name.split('.')[0]!r} for that. If this "
        "error reached you, the clone could not be made.",
        name=spec.name, path=spec.origin,
    )


_ISO_ERROR_MARKERS = (
    "does not support loading in subinterpreters",  # CPython's multi-interp check
    "cannot load module more than once per process",  # numpy's guard
    "do not yet support subinterpreters",  # PyO3 #576
    "this module can only be loaded into one interpreter per process",  # Cython
    # CPython ran PyInit in main (import.c) and it raised there, e.g. an
    # `import_array()` in PyInit hitting numpy's guard in the main interpreter
    "failed to import from subinterpreter due to exception",
    _ISO_SHARED_SINGLE_PHASE,
)


def _iso_offending_module(exc):
    """The module whose load raised an isolation-class ImportError, found along
    the cause/context chain (packages re-raise their extension's error as their
    own: scipy "The `scipy` install you are using seems to be broken", sklearn
    `raise_build_error`). "" if it is one but carries no name; None if the
    chain has no isolation-class error."""
    seen = set()
    found = None
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, ImportError) and any(m in str(exc) for m in _ISO_ERROR_MARKERS):
            if exc.name:
                return exc.name
            found = ""
        exc = exc.__cause__ or exc.__context__
    return found


def _iso_import(name, globals=None, locals=None, fromlist=(), level=0):
    global _iso_in_hook
    if _iso_in_hook:
        # Nested import (e.g. numpy/__init__ importing its own .so): let it raise
        # so the failure propagates to the outermost call, which owns the
        # isolate-and-restart of the whole top-level statement.
        return _iso_real_import(name, globals, locals, fromlist, level)
    _iso_in_hook = True
    try:
        outer = (name or "").split(".")[0] if level == 0 else ""
        cloned = set()
        while True:
            try:
                return _iso_real_import(name, globals, locals, fromlist, level)
            except ImportError as exc:
                bad = _iso_offending_module(exc)
                if bad is None:
                    raise  # unrelated ImportError — surface the real error
                top = (bad or outer).split(".")[0]
                # One clone per package per statement: a package that still
                # fails from its clone surfaces its real error, never loops.
                # pyronova is never isolated (FR-11): its error surfaces as is.
                if not top or top == "pyronova" or top in cloned or not _iso_isolate(top):
                    raise
                cloned.add(top)
                # Drop the statement's partial import too, so the retry
                # re-resolves it with the clone dir on sys.path. Never pyronova:
                # the failed submodule is already out of sys.modules, and the
                # package must stay the one this worker already executed (FR-11).
                if outer and outer != "pyronova":
                    _iso_evict(outer)
    finally:
        _iso_in_hook = False


_pyronova_isolate_libs()
# The loader's own helpers use ctypes. Import it fully BEFORE the finder is
# installed: once installed, loading `_ctypes` goes through the loader, which
# would then see a half-initialized `ctypes`.
import ctypes as _iso_ctypes
import sys as _sys_iso
_sys_iso.meta_path.insert(0, _IsolatingExtensionFinder())
_builtins.__import__ = _iso_import


# -- The engine for the logging bridge -----------------------------------------
# Imported last, with the isolation machinery above in place: importing the
# engine imports the `pyronova` package, and its own imports go through it.
from pyronova.engine import emit_python_log as _emit_python_log  # noqa: E402
