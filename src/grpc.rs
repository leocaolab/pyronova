//! Minimal gRPC unary support for the `benchmark.BenchmarkService`
//! interface used by HttpArena's `unary-grpc` / `unary-grpc-tls`
//! profiles.
//!
//! Opt-in (`app.enable_grpc_benchmark()`): when enabled, a POST to exactly
//! [`GET_SUM_PATH`] with an `application/grpc*` content-type is answered here;
//! every other request, gRPC or not, is routed like any other.
//!
//! Hand-rolled (no `tonic`/`prost` dependency) because the protobuf
//! surface is a single pair of messages:
//!
//! ```proto
//! message SumRequest  { int32 a = 1; int32 b = 2; }
//! message SumReply    { int32 result = 1; }
//! rpc   GetSum (SumRequest) returns (SumReply);
//! ```
//!
//! That's ~20 lines of varint I/O plus an HTTP/2 frame-with-trailers
//! response — cheaper in binary size and compile time than pulling in
//! the tonic stack. If the Arena spec ever adds streaming or a richer
//! message we can revisit.
//!
//! Transport notes
//! ---------------
//! * Unary gRPC over HTTP/2 (`application/grpc+proto`). Body is a
//!   single length-prefixed frame: `[0x00][u32 BE len][proto bytes]`.
//! * Response carries status via HTTP/2 **trailers** — `grpc-status`
//!   plus, on failure, a percent-encoded `grpc-message` with the real cause.
//! * ALPN `h2` is negotiated for the TLS variant; our rustls acceptor
//!   already advertises h2. For the cleartext `unary-grpc` profile the
//!   client (h2load) starts with the HTTP/2 preface which hyper's
//!   `AutoBuilder` picks up.

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{HeaderValue, InvalidHeaderValue};
use hyper::{HeaderMap, Request, Response};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

use crate::body::{read_body, BodyReject, BoxBody, REQUEST_BUDGET};

/// The canonical gRPC status codes this server emits
/// (https://grpc.github.io/grpc/core/md_doc_statuscodes.html).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GrpcStatus {
    Ok,
    DeadlineExceeded,
    ResourceExhausted,
    Unimplemented,
    Internal,
    Unavailable,
}

impl GrpcStatus {
    fn header_value(self) -> HeaderValue {
        HeaderValue::from_static(match self {
            Self::Ok => "0",
            Self::DeadlineExceeded => "4",
            Self::ResourceExhausted => "8",
            Self::Unimplemented => "12",
            Self::Internal => "13",
            Self::Unavailable => "14",
        })
    }
}

/// Why a unary call failed. The `Display` text is sent as `grpc-message`.
#[derive(Debug, thiserror::Error)]
enum GrpcError {
    #[error("request body exceeds max_body_size ({limit} bytes)")]
    BodyTooLarge { limit: usize },
    #[error("request body did not arrive within {REQUEST_BUDGET:?}")]
    BodyTimedOut,
    #[error("request body read failed: {0}")]
    BodyRead(Box<dyn std::error::Error + Send + Sync>),
    #[error("request is {len} bytes, shorter than the 5-byte gRPC frame header")]
    ShortFrame { len: usize },
    #[error("message is compressed (flag {flag}); no grpc-encoding is supported")]
    Compressed { flag: u8 },
    #[error("frame declares a {declared}-byte message but carries {actual} bytes")]
    TruncatedFrame { declared: usize, actual: usize },
    #[error(
        "frame declares a {declared}-byte message but carries {actual} bytes: a unary call \
         sends exactly one message, nothing after it"
    )]
    TrailingBytes { declared: usize, actual: usize },
    #[error("malformed SumRequest: {0}")]
    Decode(#[from] DecodeError),
}

impl GrpcError {
    fn status(&self) -> GrpcStatus {
        match self {
            Self::BodyTooLarge { .. } => GrpcStatus::ResourceExhausted,
            Self::BodyTimedOut => GrpcStatus::DeadlineExceeded,
            Self::BodyRead(_) => GrpcStatus::Unavailable,
            Self::Compressed { .. } => GrpcStatus::Unimplemented,
            Self::ShortFrame { .. }
            | Self::TruncatedFrame { .. }
            | Self::TrailingBytes { .. }
            | Self::Decode(_) => GrpcStatus::Internal,
        }
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum DecodeError {
    #[error("truncated or over-long varint")]
    Varint,
    #[error("field {field} runs past the end of the message")]
    Truncated { field: u64 },
    #[error("field {field} uses wire type {wire_type}, which proto3 does not allow")]
    WireType { field: u64, wire_type: u8 },
}

/// The one method the benchmark service implements.
pub(crate) const GET_SUM_PATH: &str = "/benchmark.BenchmarkService/GetSum";

/// A gRPC call to [`GET_SUM_PATH`]: POST to that exact path with an `application/grpc*`
/// content-type.
pub(crate) fn is_get_sum_call(req: &Request<Incoming>) -> bool {
    req.method() == hyper::Method::POST
        && req.uri().path() == GET_SUM_PATH
        && req
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("application/grpc"))
}

/// Answers the benchmark method; `limit` is the app's `max_body_size`.
pub(crate) async fn handle_get_sum(
    req: Request<Incoming>,
    limit: usize,
) -> Result<Response<BoxBody>, hyper::Error> {
    let reply = read_message(req.into_body(), limit)
        .await
        .and_then(|message| get_sum(&message));
    Ok(match reply {
        Ok(reply) => grpc_reply(Some(frame(&reply)), GrpcStatus::Ok, None),
        Err(e) => grpc_reply(None, e.status(), Some(&e.to_string())),
    })
}

/// Collect the body under the budget every request body gets (the app's size cap: a
/// multi-GB body behind an `application/grpc` content-type must not OOM the process; the
/// time budget: a stalled one must not hold the connection) and unframe it.
async fn read_message(body: Incoming, limit: usize) -> Result<Bytes, GrpcError> {
    let collected = read_body(body, limit)
        .await
        .map_err(|reject| match reject {
            BodyReject::TooLarge => GrpcError::BodyTooLarge { limit },
            BodyReject::TimedOut => GrpcError::BodyTimedOut,
            BodyReject::Read(e) => GrpcError::BodyRead(Box::new(e)),
        })?;
    unframe(collected)
}

/// `[compressed flag: u8][length: u32 BE][message]` → message.
fn unframe(framed: Bytes) -> Result<Bytes, GrpcError> {
    const HEADER_LEN: usize = 5;
    if framed.len() < HEADER_LEN {
        return Err(GrpcError::ShortFrame { len: framed.len() });
    }
    if framed[0] != 0 {
        return Err(GrpcError::Compressed { flag: framed[0] });
    }
    let declared = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]) as usize;
    let actual = framed.len() - HEADER_LEN;
    match actual.cmp(&declared) {
        std::cmp::Ordering::Less => Err(GrpcError::TruncatedFrame { declared, actual }),
        std::cmp::Ordering::Greater => Err(GrpcError::TrailingBytes { declared, actual }),
        std::cmp::Ordering::Equal => Ok(framed.slice(HEADER_LEN..)),
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SumRequest {
    a: i32,
    b: i32,
}

fn get_sum(message: &[u8]) -> Result<Bytes, GrpcError> {
    let SumRequest { a, b } = decode_sum_request(message)?;
    // SumReply { int32 result = 1; }  →  0x08 (field 1, varint) + varint.
    // proto3 int32 serializes negatives as 10-byte sign-extended varints,
    // matching what tonic / grpc-go emit.
    let mut reply = BytesMut::with_capacity(16);
    reply.put_u8(0x08);
    write_varint_i32(&mut reply, a.wrapping_add(b));
    Ok(reply.freeze())
}

/// proto3 wire types (https://protobuf.dev/programming-guides/encoding/#structure).
mod wire {
    pub const VARINT: u8 = 0;
    pub const I64: u8 = 1;
    pub const LEN: u8 = 2;
    pub const I32: u8 = 5;
}

/// Decode `SumRequest`. Unknown fields of any proto3 wire type are skipped, as the
/// proto3 spec requires; the deprecated group types (3, 4) and invalid ones are errors.
fn decode_sum_request(mut cursor: &[u8]) -> Result<SumRequest, DecodeError> {
    let mut request = SumRequest::default();
    while !cursor.is_empty() {
        let (tag, rest) = read_varint(cursor).ok_or(DecodeError::Varint)?;
        let (field, wire_type) = (tag >> 3, (tag & 0x07) as u8);
        cursor = match (field, wire_type) {
            (1 | 2, wire::VARINT) => {
                let (value, rest) = read_varint(rest).ok_or(DecodeError::Varint)?;
                // int32 on the wire: the low 32 bits of a sign-extended varint.
                let value = value as i32;
                if field == 1 {
                    request.a = value;
                } else {
                    request.b = value;
                }
                rest
            }
            _ => skip_field(field, wire_type, rest)?,
        };
    }
    Ok(request)
}

fn skip_field(field: u64, wire_type: u8, input: &[u8]) -> Result<&[u8], DecodeError> {
    let len = match wire_type {
        wire::VARINT => {
            return read_varint(input)
                .map(|(_, rest)| rest)
                .ok_or(DecodeError::Varint)
        }
        wire::I64 => 8,
        wire::I32 => 4,
        wire::LEN => {
            let (len, rest) = read_varint(input).ok_or(DecodeError::Varint)?;
            let len = usize::try_from(len).map_err(|_| DecodeError::Truncated { field })?;
            return rest.get(len..).ok_or(DecodeError::Truncated { field });
        }
        _ => return Err(DecodeError::WireType { field, wire_type }),
    };
    input.get(len..).ok_or(DecodeError::Truncated { field })
}

/// Prefix a message with the 5-byte gRPC frame header.
fn frame(message: &[u8]) -> Bytes {
    let mut framed = BytesMut::with_capacity(5 + message.len());
    framed.put_u8(0);
    framed.put_u32(message.len() as u32);
    framed.extend_from_slice(message);
    framed.freeze()
}

/// `grpc-message` is percent-encoded UTF-8: everything outside printable ASCII, plus
/// `%` itself (gRPC over HTTP/2 spec, "Responses").
const GRPC_MESSAGE_ENCODE: &AsciiSet = &CONTROLS.add(b'%');

fn grpc_message_value(message: &str) -> Result<HeaderValue, InvalidHeaderValue> {
    HeaderValue::try_from(utf8_percent_encode(message, GRPC_MESSAGE_ENCODE).to_string())
}

fn grpc_reply(data: Option<Bytes>, status: GrpcStatus, message: Option<&str>) -> Response<BoxBody> {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", status.header_value());
    // Percent-encoding leaves only printable ASCII, so the Err arm is unreachable in
    // practice; if it ever fires, the real message still reaches the log.
    match message.map(|m| (m, grpc_message_value(m))) {
        Some((_, Ok(value))) => {
            trailers.insert("grpc-message", value);
        }
        Some((message, Err(e))) => {
            tracing::error!(target: "pyronova::server", error = %e, message,
                "grpc-message not header-safe after percent-encoding");
        }
        None => {}
    }

    let data_frame = data.map(Frame::data);
    let trailer_frame: Frame<Bytes> = Frame::trailers(trailers);

    let body = StreamBody::new(stream::iter(
        data_frame
            .into_iter()
            .chain(std::iter::once(trailer_frame))
            .map(Ok::<_, hyper::Error>),
    ));
    let boxed: BoxBody = body.boxed();

    // Build infallibly: Response::new defaults to 200 OK, and inserting a
    // 'static header value never errors — no unwrap / panic path.
    let mut response = Response::new(boxed);
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/grpc"));
    response
}

fn read_varint(input: &[u8]) -> Option<(u64, &[u8])> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in input.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, &input[i + 1..]));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

fn write_varint_i32(buf: &mut BytesMut, value: i32) {
    // proto3 int32 with negative value sign-extends to u64 before
    // encoding; positive values encode their natural magnitude.
    let mut v = value as i64 as u64;
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            buf.put_u8(byte);
            return;
        }
        buf.put_u8(byte | 0x80);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip_positive() {
        for v in [0_i32, 1, 127, 128, 12345, 1 << 30] {
            let mut buf = BytesMut::new();
            write_varint_i32(&mut buf, v);
            let (decoded, rest) = read_varint(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded as i32, v, "value {v}");
        }
    }

    #[test]
    fn varint_negative_is_10_bytes() {
        let mut buf = BytesMut::new();
        write_varint_i32(&mut buf, -1);
        // -1 sign-extended to u64 is all-ones → 10-byte varint.
        assert_eq!(buf.len(), 10);
        let (decoded, _) = read_varint(&buf).unwrap();
        assert_eq!(decoded as i32, -1);
    }

    #[test]
    fn read_varint_stops_at_shift_overflow() {
        // 11 bytes, all continuation set — should bail rather than UB.
        let bad = vec![0xFF; 11];
        assert!(read_varint(&bad).is_none());
    }

    async fn trailers(resp: Response<BoxBody>) -> HeaderMap {
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .trailers()
            .cloned()
            .unwrap()
    }

    #[test]
    fn unknown_fields_of_every_proto3_wire_type_are_skipped() {
        let message = [
            0x08, 0x02, // a = 2
            0x1a, 0x02, b'h', b'i', // field 3, LEN "hi"
            0x21, 1, 2, 3, 4, 5, 6, 7, 8, // field 4, I64
            0x2d, 1, 2, 3, 4, // field 5, I32
            0x30, 0x7f, // field 6, VARINT
            0x80, 0x01, 0x00, // field 16 (two-byte tag), VARINT 0
            0x10, 0x03, // b = 3
        ];
        assert_eq!(decode_sum_request(&message), Ok(SumRequest { a: 2, b: 3 }));
    }

    #[test]
    fn group_and_invalid_wire_types_are_errors() {
        assert_eq!(
            decode_sum_request(&[0x1b]),
            Err(DecodeError::WireType {
                field: 3,
                wire_type: 3
            })
        );
        assert_eq!(
            decode_sum_request(&[0x1e]),
            Err(DecodeError::WireType {
                field: 3,
                wire_type: 6
            })
        );
    }

    #[test]
    fn truncated_unknown_field_is_an_error() {
        assert_eq!(
            decode_sum_request(&[0x1a, 0x05, b'h']),
            Err(DecodeError::Truncated { field: 3 })
        );
        assert_eq!(
            decode_sum_request(&[0x21, 1, 2]),
            Err(DecodeError::Truncated { field: 4 })
        );
    }

    #[test]
    fn negative_int32_sums_wrap() {
        let mut message = BytesMut::new();
        message.put_u8(0x08);
        write_varint_i32(&mut message, i32::MAX);
        message.put_u8(0x10);
        write_varint_i32(&mut message, 1);
        let reply = get_sum(&message).unwrap();
        let (value, _) = read_varint(&reply[1..]).unwrap();
        assert_eq!(value as i32, i32::MIN);
    }

    #[test]
    fn body_read_error_carries_the_real_cause() {
        let cause: Box<dyn std::error::Error + Send + Sync> = "connection reset by peer".into();
        let e = GrpcError::BodyRead(cause);
        assert_eq!(e.status(), GrpcStatus::Unavailable);
        assert!(e.to_string().contains("connection reset by peer"), "{e}");
    }

    #[test]
    fn frame_errors_say_what_was_wrong() {
        let e = unframe(Bytes::from_static(&[0, 0, 0, 0, 9, 1])).unwrap_err();
        assert_eq!(
            e.to_string(),
            "frame declares a 9-byte message but carries 1 bytes"
        );
        assert_eq!(e.status(), GrpcStatus::Internal);
        let e = unframe(Bytes::from_static(&[1, 0, 0, 0, 0])).unwrap_err();
        assert_eq!(e.status(), GrpcStatus::Unimplemented);
    }

    #[test]
    fn bytes_after_the_message_are_an_error() {
        let e = unframe(Bytes::from_static(&[0, 0, 0, 0, 2, 8, 1, 0xff])).unwrap_err();
        assert!(matches!(
            e,
            GrpcError::TrailingBytes {
                declared: 2,
                actual: 3
            }
        ));
        assert_eq!(e.status(), GrpcStatus::Internal);
        assert_eq!(
            unframe(Bytes::from_static(&[0, 0, 0, 0, 2, 8, 1])).unwrap(),
            Bytes::from_static(&[8, 1])
        );
    }

    #[test]
    fn a_stalled_body_is_deadline_exceeded() {
        assert_eq!(
            GrpcError::BodyTimedOut.status().header_value(),
            HeaderValue::from_static("4")
        );
    }

    #[tokio::test]
    async fn multiline_non_ascii_message_is_percent_encoded_not_dropped() {
        let resp = grpc_reply(
            None,
            GrpcStatus::Internal,
            Some("line one\nline two: é 100%"),
        );
        let t = trailers(resp).await;
        assert_eq!(t.get("grpc-status").unwrap(), "13");
        assert_eq!(
            t.get("grpc-message").unwrap(),
            "line one%0Aline two: %C3%A9 100%25"
        );
    }

    #[tokio::test]
    async fn ok_reply_has_no_message() {
        let t = trailers(grpc_reply(None, GrpcStatus::Ok, None)).await;
        assert_eq!(t.get("grpc-status").unwrap(), "0");
        assert!(t.get("grpc-message").is_none());
    }
}
