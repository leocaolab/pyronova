"""The client-facing error policy, for the handlers pyronova itself
registers in Python (rpc, crud, health, mcp). The engine applies the same policy to
every handler, hook and worker error.

A 4xx body carries the reason: the client caused it and can fix it. A 5xx body is
generic plus the request id; the exception and its traceback go to the log with the
same id, so an operator can find it from what the client reports. Exception text never
reaches the client: it can embed paths, SQL, connection strings or config values.
"""

from __future__ import annotations

import logging

GENERIC_500 = "Internal Server Error"


def server_error_body(request_id: str) -> dict:
    """The body fields of a 5xx: the generic text and the request id."""
    return {"error": GENERIC_500, "request_id": request_id}


def log_server_error(logger: logging.Logger, request_id: str, message: str, *args) -> None:
    """Logs the exception being handled, with its traceback and the request id."""
    logger.exception(message + " (request_id=%s)", *args, request_id)
