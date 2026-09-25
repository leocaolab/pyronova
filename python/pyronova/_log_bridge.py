"""Python ``logging`` → Rust ``tracing``: the one handler every interpreter installs.

The main interpreter imports this as ``pyronova._log_bridge``. A worker runs the same
source in its bootstrap namespace (the engine embeds it), because the bootstrap must
route logging before the ``pyronova`` package can be imported. So it imports only the
standard library.
"""

import logging
import sys

# Formats a record's exception (`logger.exception`) as its traceback. A `Handler` has no
# `formatException`; that is a `Formatter` method.
_TRACEBACK_FORMAT = logging.Formatter()


class RustLogHandler(logging.Handler):
    """Routes each record to ``pyronova.engine.emit_python_log``, tagged with
    ``worker_id`` (``None`` on the main interpreter, which records no worker field).

    Until ``connect(emit)`` gives it the engine's function, records go to stderr: a
    worker logs during its bootstrap, before the engine can be imported."""

    def __init__(self, worker_id, emit=None):
        super().__init__()
        self._worker_id = worker_id
        self._sink = emit

    def connect(self, emit):
        self._sink = emit

    def emit(self, record):
        try:
            msg = record.getMessage()
            # A local, not `record.exc_text`: the same record may go to other handlers.
            if record.exc_info:
                exc_text = record.exc_text or _TRACEBACK_FORMAT.formatException(record.exc_info)
                msg = f"{msg}\n{exc_text}"
            if self._sink is None:
                sys.stderr.write(f"{record.levelname} {record.name}: {msg}\n")
                return
            self._sink(
                record.levelno,
                record.name,
                msg,
                record.pathname or "",
                record.lineno or 0,
                self._worker_id,
            )
        except Exception:
            # Never crash the caller for a log line. `handleError` is logging's own
            # "emitting failed" hook: it honours `logging.raiseExceptions` and writes the
            # failing record to stderr.
            self.handleError(record)


def root_level(engine_level):
    """The root logger's level for the engine's ``_python_log_level()``: records below
    what Rust logs are dropped before formatting or the FFI call. Before any
    ``init_logger`` (``None``, a raw engine app) every record goes to Rust, whose filter
    decides."""
    return logging.DEBUG if engine_level is None else engine_level
