//! A logged error as the response the client gets (decision D4): a 4xx body carries the
//! reason; a 5xx body is generic plus the request id, which is also on the log line
//! holding the real error. The error itself is `crate::error`.

use bytes::Bytes;
use http_body_util::Full;
use hyper::Response;

use crate::body::BodyReject;
use crate::error::{HandlerError, Logged, LoggedError, Refusal};
use crate::response;

/// The body text of every 500.
pub(crate) const GENERIC_500: &str = "Internal Server Error";

impl Logged {
    /// The response for this error. This match is the one place that maps an error to its
    /// status. Rendering only: the error was logged, and a refusal counted, where it
    /// happened.
    pub(crate) fn into_response(self) -> Response<Full<Bytes>> {
        let LoggedError { error, request_id } = self.into_inner();
        match error {
            HandlerError::Refused(refused) => match refused.refusal() {
                Refusal::Overloaded(_) => {
                    response::overloaded_response("server overloaded", &request_id)
                }
                Refusal::PoolClosed(_) => {
                    response::unavailable_response("server shutting down", &request_id)
                }
                Refusal::Timeout | Refusal::Overran { .. } => {
                    response::gateway_timeout_response(&request_id)
                }
                Refusal::WorkerLost(_) => response::error_response(GENERIC_500, &request_id),
            },
            HandlerError::BodyRejected(BodyReject::TooLarge) => {
                response::payload_too_large_response()
            }
            HandlerError::BodyRejected(BodyReject::TimedOut) => {
                response::request_timeout_response()
            }
            HandlerError::BodyRejected(reject @ BodyReject::Read(_)) => {
                response::bad_request_response(&reject.to_string())
            }
            HandlerError::Python { .. }
            | HandlerError::Panic { .. }
            | HandlerError::Response(_)
            | HandlerError::ThreadSpawn(_)
            | HandlerError::ForeignEventLoop { .. } => {
                response::error_response(GENERIC_500, &request_id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::refuse;
    use crate::error::tests::{captured_log_value, tag};
    use crate::request_id::RequestId;

    fn body(resp: Response<Full<Bytes>>) -> serde_json::Value {
        use http_body_util::BodyExt;
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let bytes = rt.block_on(resp.into_body().collect()).unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn a_5xx_body_is_generic_with_the_logged_id() {
        let id = RequestId::mint();
        let cases = [
            (
                HandlerError::Panic {
                    payload: "secret payload".into(),
                },
                500,
                GENERIC_500,
            ),
            (
                refuse(Refusal::WorkerLost("secret worker")),
                500,
                GENERIC_500,
            ),
            (
                refuse(Refusal::Overloaded("secret queue")),
                503,
                "server overloaded",
            ),
            (
                refuse(Refusal::PoolClosed("secret pool")),
                503,
                "server shutting down",
            ),
            (refuse(Refusal::Timeout), 504, "request timeout"),
        ];
        for (err, status, text) in cases {
            let resp = captured_log_value(|| err.log(&tag(&id)).into_response());
            assert_eq!(resp.status().as_u16(), status);
            let body = body(resp);
            assert_eq!(
                body,
                serde_json::json!({"error": text, "request_id": id.to_string()})
            );
        }
    }

    #[test]
    fn a_4xx_body_carries_the_reason() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            HandlerError::BodyRejected(BodyReject::TooLarge)
                .log(&tag(&id))
                .into_response()
        });
        assert_eq!(resp.status().as_u16(), 413);
        assert_eq!(
            body(resp),
            serde_json::json!({"error": "payload too large"})
        );
        let resp = captured_log_value(|| {
            HandlerError::BodyRejected(BodyReject::TimedOut)
                .log(&tag(&id))
                .into_response()
        });
        assert_eq!(resp.status().as_u16(), 408);
        assert_eq!(
            body(resp),
            serde_json::json!({"error": "request body timeout"})
        );
    }

    #[test]
    fn rendering_does_not_count_a_dropped_request() {
        use std::sync::atomic::Ordering::Relaxed;
        // Each refusal was counted once, when it was made; rendering adds nothing.
        let id = RequestId::mint();
        let logged: Vec<_> = (0..50)
            .map(|_| captured_log_value(|| refuse(Refusal::Timeout).log(&tag(&id))))
            .collect();
        let before = crate::monitor::DROPPED_REQUESTS.load(Relaxed);
        for l in logged {
            let _ = l.into_response();
        }
        // Other tests may refuse concurrently, but not 50 times; a renderer that counted
        // would add exactly one per response.
        let after = crate::monitor::DROPPED_REQUESTS.load(Relaxed);
        assert!(after - before < 50, "{before} -> {after}");
    }

    #[test]
    fn a_foreign_event_loop_is_a_generic_500() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            HandlerError::ForeignEventLoop {
                loop_interp: 0,
                current: 3,
            }
            .log(&tag(&id))
            .into_response()
        });
        assert_eq!(resp.status().as_u16(), 500);
        assert_eq!(
            body(resp),
            serde_json::json!({"error": GENERIC_500, "request_id": id.to_string()})
        );
    }

    #[test]
    fn a_logged_error_falls_back_to_500() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            refuse(Refusal::WorkerLost("oops"))
                .log(&tag(&id))
                .into_response()
        });
        assert_eq!(resp.status().as_u16(), 500);
    }
}
