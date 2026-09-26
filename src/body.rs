//! HTTP bodies: a request body as the server reads it — buffered whole, or streamed to
//! the handler chunk by chunk — within one budget, and the body type every response has.
//!
//! The bottom layer: the pipeline, the Python `BodyStream` and gRPC build on it, and it
//! imports none of them.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::Response;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// How long the server waits for one step of a request — its whole body arriving, or a
/// handler producing its response — before it gives up on it.
pub(crate) const REQUEST_BUDGET: Duration = Duration::from_secs(30);

// ─────────────────────────── responses ───────────────────────────

/// The body of every response the server sends.
pub(crate) type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

#[inline]
pub(crate) fn full_body(resp: Response<Full<Bytes>>) -> Response<BoxBody> {
    // `Full`'s error type is `Infallible`: `match e {}` converts it without a panic path.
    resp.map(|b| b.map_err(|e| match e {}).boxed())
}

// ─────────────────────────── rejection ───────────────────────────

/// Why a request body was not read, buffered or streamed: the client's doing (a 4xx).
/// `Clone` (the read error is shared) so a streamed body's rejection can travel through the
/// handler as a Python exception and back (`python::body_stream::BodyRejected`).
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum BodyReject {
    #[error("request body is larger than max_body_size")]
    TooLarge,
    #[error("request body did not arrive within {REQUEST_BUDGET:?}")]
    TimedOut,
    #[error("request body read failed: {0}")]
    Read(#[source] Arc<hyper::Error>),
}

// ─────────────────────────── buffered ───────────────────────────

/// What a body must pass, besides the size cap, as it is buffered; its error type says
/// what refusing it means.
pub(crate) trait BodyGate {
    type Error: From<BodyReject>;
    /// Called with the bytes buffered so far, after each frame.
    fn admit(&mut self, buffered: usize) -> Result<(), Self::Error>;
}

/// No gate: only the size cap and the budget.
struct Ungated;

impl BodyGate for Ungated {
    type Error = BodyReject;
    fn admit(&mut self, _buffered: usize) -> Result<(), BodyReject> {
        Ok(())
    }
}

/// The whole body, at most `max` bytes, within [`REQUEST_BUDGET`].
pub(crate) async fn read_body(body: Incoming, max: usize) -> Result<Bytes, BodyReject> {
    collect(body, max, &mut Ungated).await
}

/// [`read_body`], passing `gate` after each frame.
pub(crate) async fn collect<G: BodyGate>(
    mut body: Incoming,
    max: usize,
    gate: &mut G,
) -> Result<Bytes, G::Error> {
    let read = async {
        let mut buf = BodyBuf::Empty;
        while let Some(frame) = body.frame().await {
            // Trailer frames carry no body bytes.
            let Ok(data) = frame
                .map_err(|e| BodyReject::Read(Arc::new(e)))?
                .into_data()
            else {
                continue;
            };
            if buf.len().saturating_add(data.len()) > max {
                return Err(BodyReject::TooLarge.into());
            }
            buf = buf.push(data);
            gate.admit(buf.len())?;
        }
        Ok::<_, G::Error>(buf.freeze())
    };
    tokio::time::timeout(REQUEST_BUDGET, read)
        .await
        .map_err(|_| BodyReject::TimedOut)?
}

/// Body bytes as they arrive. A one-frame body (the common case) is kept as hyper handed
/// it over, with no copy.
enum BodyBuf {
    Empty,
    One(Bytes),
    Many(BytesMut),
}

impl BodyBuf {
    fn len(&self) -> usize {
        match self {
            BodyBuf::Empty => 0,
            BodyBuf::One(b) => b.len(),
            BodyBuf::Many(b) => b.len(),
        }
    }

    fn push(self, data: Bytes) -> Self {
        match self {
            BodyBuf::Empty => BodyBuf::One(data),
            BodyBuf::One(first) => {
                let mut joined = BytesMut::with_capacity(first.len() + data.len());
                joined.extend_from_slice(&first);
                joined.extend_from_slice(&data);
                BodyBuf::Many(joined)
            }
            BodyBuf::Many(mut joined) => {
                joined.extend_from_slice(&data);
                BodyBuf::Many(joined)
            }
        }
    }

    fn freeze(self) -> Bytes {
        match self {
            BodyBuf::Empty => Bytes::new(),
            BodyBuf::One(b) => b,
            BodyBuf::Many(b) => b.freeze(),
        }
    }
}

// ─────────────────────────── streamed ───────────────────────────

/// Chunks in flight between the feeder and the handler. Low enough to enforce real
/// backpressure on a slow consumer, high enough that a steady-state 64 KB-frame stream
/// never stalls on round-trip scheduling latency.
const CHANNEL_CAPACITY: usize = 8;

/// A message on the feeder → handler channel.
pub(crate) enum ChunkMsg {
    Data(Bytes),
    /// The feeder gave up on the body; reading it raises `BodyRejected`.
    Err(BodyReject),
    /// End of body; the feeder drops its sender after it.
    Eof,
}

pub(crate) type BodySender = mpsc::Sender<ChunkMsg>;
pub(crate) type BodyReceiver = mpsc::Receiver<ChunkMsg>;

/// A bounded channel for one streamed body: the bound carries a slow handler's
/// backpressure back to the client's TCP window.
pub(crate) fn body_channel() -> (BodySender, BodyReceiver) {
    mpsc::channel(CHANNEL_CAPACITY)
}

/// `req.stream` of one request: a buffered body has none; a streamed body's receiver is
/// handed out once.
pub(crate) enum StreamSlot {
    Buffered,
    Streamed(Mutex<Option<BodyReceiver>>),
}

/// `req.stream` was read a second time: the first read took the stream.
#[derive(Debug, thiserror::Error)]
#[error("req.stream was already taken: a streamed body is read through one stream")]
pub(crate) struct StreamTaken;

impl StreamSlot {
    pub(crate) fn streamed(rx: BodyReceiver) -> Self {
        StreamSlot::Streamed(Mutex::new(Some(rx)))
    }

    /// The stream: `None` for a buffered body, the receiver the first time for a
    /// streamed one, [`StreamTaken`] after that.
    pub(crate) fn take(&self) -> Result<Option<BodyReceiver>, StreamTaken> {
        match self {
            StreamSlot::Buffered => Ok(None),
            // A panic while the lock was held left the `Option` whole.
            StreamSlot::Streamed(slot) => slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .map(Some)
                .ok_or(StreamTaken),
        }
    }
}

/// How feeding a streamed body ended, when the handler was there for all of it.
enum Fed {
    /// The whole body was handed over.
    Complete,
    Rejected(BodyReject),
}

/// The handler dropped the stream, or did not take a chunk before the deadline.
struct HandlerGone;

/// Feeds a `stream=True` route's body to its handler, one frame at a time, through `tx`.
///
/// The whole body is bounded like a buffered one: at most `max_size` bytes, all of it
/// within [`REQUEST_BUDGET`] of the feeder starting. Every send to the handler, the
/// body's end included, waits at most until that same deadline, so a handler that stops
/// reading cannot hold the feeder. A body it gives up on ends with the same
/// [`BodyReject`] a buffered body gets, so it answers the same 413/408/400; a handler that
/// stopped reading sees the stream end without its end, which reading it then reports.
pub(crate) async fn stream_body_feeder(body: Incoming, tx: BodySender, max_size: usize) {
    let deadline = Instant::now() + REQUEST_BUDGET;
    let last = match feed(body, &tx, max_size, deadline).await {
        Ok(Fed::Complete) => ChunkMsg::Eof,
        Ok(Fed::Rejected(reject)) => ChunkMsg::Err(reject),
        Err(HandlerGone) => return,
    };
    if hand(&tx, last, deadline).await.is_err() {
        tracing::debug!(target: "pyronova::server",
            "the handler stopped reading its streamed body before its end");
    }
}

async fn feed(
    mut body: Incoming,
    tx: &BodySender,
    max_size: usize,
    deadline: Instant,
) -> Result<Fed, HandlerGone> {
    use hyper::body::Body;
    let mut total: usize = 0;
    loop {
        let next = tokio::time::timeout_at(
            deadline,
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)),
        )
        .await;
        let frame = match next {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(e))) => return Ok(Fed::Rejected(BodyReject::Read(Arc::new(e)))),
            Ok(None) => return Ok(Fed::Complete),
            Err(_) => return Ok(Fed::Rejected(BodyReject::TimedOut)),
        };
        // Trailer / metadata frames carry no body bytes.
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        total = total.saturating_add(chunk.len());
        if total > max_size {
            return Ok(Fed::Rejected(BodyReject::TooLarge));
        }
        // A slow handler blocks this send, which stops the next poll_frame, which closes
        // the client's TCP window: backpressure all the way to the wire.
        hand(tx, ChunkMsg::Data(chunk), deadline).await?;
    }
}

/// Hands `msg` to the handler, waiting for room until `deadline` at most. A message that
/// fits is sent even once the deadline has passed (the send is tried first), so a
/// rejection for running out of time still reaches a handler that keeps up.
async fn hand(tx: &BodySender, msg: ChunkMsg, deadline: Instant) -> Result<(), HandlerGone> {
    match tokio::time::timeout_at(deadline, tx.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) | Err(_) => Err(HandlerGone),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_buffered_request_has_no_stream() {
        assert!(matches!(StreamSlot::Buffered.take(), Ok(None)));
    }

    #[test]
    fn a_streamed_body_is_taken_once() {
        let (_tx, rx) = body_channel();
        let slot = StreamSlot::streamed(rx);
        assert!(matches!(slot.take(), Ok(Some(_))));
        assert!(matches!(slot.take(), Err(StreamTaken)));
    }

    #[tokio::test]
    async fn a_send_to_a_handler_that_stopped_reading_gives_up_at_the_deadline() {
        let (tx, _rx) = body_channel();
        let deadline = Instant::now() + Duration::from_millis(50);
        // Fill the channel: nobody reads.
        for _ in 0..CHANNEL_CAPACITY {
            assert!(hand(&tx, ChunkMsg::Data(Bytes::new()), deadline)
                .await
                .is_ok());
        }
        // The final Eof waits no longer than the deadline.
        let gave_up =
            tokio::time::timeout(Duration::from_secs(5), hand(&tx, ChunkMsg::Eof, deadline));
        assert!(matches!(gave_up.await, Ok(Err(HandlerGone))));
        assert!(Instant::now() >= deadline);
    }

    #[tokio::test]
    async fn a_message_that_fits_is_sent_past_the_deadline() {
        let (tx, mut rx) = body_channel();
        let past = Instant::now();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let sent = hand(&tx, ChunkMsg::Err(BodyReject::TimedOut), past).await;
        assert!(sent.is_ok());
        assert!(matches!(
            rx.recv().await,
            Some(ChunkMsg::Err(BodyReject::TimedOut))
        ));
    }
}
