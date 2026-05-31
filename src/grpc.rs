//! Minimal gRPC unary support for the `benchmark.BenchmarkService`
//! interface used by HttpArena's `unary-grpc` / `unary-grpc-tls`
//! profiles.
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
//! * Response carries status via HTTP/2 **trailers** — `grpc-status: 0`
//!   for success, `13` for internal error, `12` for unimplemented.
//! * ALPN `h2` is negotiated for the TLS variant; our rustls acceptor
//!   already advertises h2. For the cleartext `unary-grpc` profile the
//!   client (h2load) starts with the HTTP/2 preface which hyper's
//!   `AutoBuilder` picks up.

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream;
use http_body_util::{BodyExt, LengthLimitError, Limited, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::HeaderValue;
use hyper::{HeaderMap, Request, Response};

use crate::handlers::BoxBody;

/// Canonical gRPC status codes. We only need the handful the server
/// actually emits — the full list is at
/// https://grpc.github.io/grpc/core/md_doc_statuscodes.html.
mod status {
    pub const OK: &str = "0";
    pub const UNIMPLEMENTED: &str = "12";
    pub const INTERNAL: &str = "13";
    pub const UNAVAILABLE: &str = "14";
    pub const RESOURCE_EXHAUSTED: &str = "8";
}

/// Is this a gRPC call? Matches POST with an `application/grpc*`
/// content-type. Caller has already routed on HTTP/2 upgrade so we
/// trust the outer transport.
pub(crate) fn is_grpc_request(req: &Request<Incoming>) -> bool {
    if req.method() != hyper::Method::POST {
        return false;
    }
    let ct = req
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    ct.starts_with("application/grpc")
}

pub(crate) async fn handle_grpc(req: Request<Incoming>) -> Result<Response<BoxBody>, hyper::Error> {
    let path = req.uri().path().to_string();
    // Guard body read with the same size cap the rest of the server
    // uses. gRPC is normally small messages (< 4 KB for the Arena
    // GetSum proto) but we're serving on the same ports as HTTP — a
    // malicious client pushing a multi-GB body through an
    // `application/grpc` content-type would OOM the process without
    // this cap. `Limited::collect` yields an Error once the cap is
    // crossed, and we fold that into `RESOURCE_EXHAUSTED` (gRPC
    // status 8) for the caller.
    let limited = Limited::new(req.into_body(), crate::handlers::max_body_size());
    let collected = match limited.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            // Distinguish "client exceeded our size cap" from a transport
            // drop so the grpc-status on the wire actually matches what
            // happened. Downcasting the boxed error is the documented way
            // to interrogate `Limited` (see its rustdoc example).
            return Ok(if e.downcast_ref::<LengthLimitError>().is_some() {
                grpc_reply_trailers(None, status::RESOURCE_EXHAUSTED, "body too large")
            // RESOURCE_EXHAUSTED
            } else {
                grpc_reply_trailers(None, status::UNAVAILABLE, "body read failed")
                // UNAVAILABLE
            });
        }
    };

    if collected.len() < 5 {
        return Ok(grpc_reply_trailers(None, status::INTERNAL, "short frame"));
    }
    if collected[0] != 0 {
        // Non-zero = compressed. We don't advertise or understand any
        // codec beyond identity; tell the caller.
        return Ok(grpc_reply_trailers(
            None,
            status::UNIMPLEMENTED,
            "compression unsupported",
        ));
    }
    let payload_len =
        u32::from_be_bytes([collected[1], collected[2], collected[3], collected[4]]) as usize;
    if collected.len() < 5 + payload_len {
        return Ok(grpc_reply_trailers(
            None,
            status::INTERNAL,
            "truncated frame",
        ));
    }
    let payload = &collected[5..5 + payload_len];

    match path.as_str() {
        "/benchmark.BenchmarkService/GetSum" => get_sum(payload),
        _ => Ok(grpc_reply_trailers(
            None,
            status::UNIMPLEMENTED,
            "unimplemented",
        )),
    }
}

fn get_sum(payload: &[u8]) -> Result<Response<BoxBody>, hyper::Error> {
    let (mut a, mut b) = (0i64, 0i64);
    let mut cursor = payload;
    while !cursor.is_empty() {
        let tag = cursor[0];
        cursor = &cursor[1..];
        let wire_type = tag & 0x07;
        let field_no = tag >> 3;
        let Some((val, rest)) = read_varint(cursor) else {
            return Ok(grpc_reply_trailers(None, status::INTERNAL, "bad varint"));
        };
        match (field_no, wire_type) {
            (1, 0) => a = val as i64,
            (2, 0) => b = val as i64,
            _ => {
                // Unknown / non-varint field — proto3 says skip.
                if wire_type != 0 {
                    return Ok(grpc_reply_trailers(
                        None,
                        status::INTERNAL,
                        "unsupported wire type",
                    ));
                }
            }
        }
        cursor = rest;
    }

    let sum = (a as i32).wrapping_add(b as i32) as i64;

    // SumReply { int32 result = 1; }  →  0x08 (field 1, varint) + varint.
    // proto3 int32 serializes negatives as 10-byte sign-extended varints,
    // matching what tonic / grpc-go emit.
    let mut reply = BytesMut::with_capacity(16);
    reply.put_u8(0x08);
    write_varint_i32(&mut reply, sum as i32);
    let reply_bytes = reply.freeze();

    // Prefix with the 5-byte gRPC frame header.
    let mut framed = BytesMut::with_capacity(5 + reply_bytes.len());
    framed.put_u8(0);
    framed.put_u32(reply_bytes.len() as u32);
    framed.extend_from_slice(&reply_bytes);

    Ok(grpc_reply_trailers(Some(framed.freeze()), status::OK, ""))
}

fn grpc_reply_trailers(
    data: Option<Bytes>,
    grpc_status: &str,
    grpc_message: &str,
) -> Response<BoxBody> {
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "grpc-status",
        HeaderValue::from_str(grpc_status).unwrap_or(HeaderValue::from_static("0")),
    );
    if !grpc_message.is_empty() {
        if let Ok(msg) = HeaderValue::from_str(grpc_message) {
            trailers.insert("grpc-message", msg);
        }
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
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/grpc"),
    );
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
}
