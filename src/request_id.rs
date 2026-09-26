//! A request's correlation id: the one value a 5xx body reports, the error log line
//! carries, the handler reads as `req.request_id` and `app.enable_request_id()` echoes.
//!
//! It is written once per request, when the pipeline resolves it to a handler (or, for a
//! `Request` built in Python, when it is constructed); everything else reads it. It is
//! the client's id when the app named a request-id header and the request carries a
//! usable one, otherwise one minted here.

use std::cell::Cell;
use std::fmt;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

use hyper::header::{HeaderMap, HeaderName, HeaderValue};

/// A client id longer than this is not used as the request id (a minted one is).
const MAX_CLIENT_ID_LEN: usize = 128;

/// Minted ids of one thread are `thread << THREAD_SHIFT | n`: 2^40 per thread before they
/// run into the next thread's range.
const THREAD_SHIFT: u32 = 40;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RequestId {
    /// Minted by this process.
    Minted(u64),
    /// Sent by the client in the app's request-id header.
    Client(HeaderValue),
}

/// Distinguishes this process's minted ids from another's (restarts, replicas).
static PROCESS_SEED: LazyLock<u64> = LazyLock::new(|| RandomState::new().hash_one(0u8));

/// The next thread's range of minted ids.
static NEXT_THREAD: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static NEXT_ID: Cell<u64> = Cell::new(NEXT_THREAD.fetch_add(1, Ordering::Relaxed) << THREAD_SHIFT);
}

impl RequestId {
    /// A fresh id. Per-thread counter: no shared cache line, no allocation.
    #[inline]
    pub(crate) fn mint() -> Self {
        RequestId::Minted(NEXT_ID.with(|next| {
            let n = next.get();
            next.set(n.wrapping_add(1));
            n
        }))
    }

    /// The id of a request with `headers`: the client's value of `header` when the app
    /// named one and it is usable (visible ASCII, 1..=128 bytes), otherwise minted.
    #[inline]
    pub(crate) fn of(headers: &HeaderMap, header: Option<&HeaderName>) -> Self {
        header
            .and_then(|name| headers.get(name))
            .filter(|value| is_usable(value))
            .map(|value| RequestId::Client(value.clone()))
            .unwrap_or_else(RequestId::mint)
    }
}

fn is_usable(value: &HeaderValue) -> bool {
    (1..=MAX_CLIENT_ID_LEN).contains(&value.len()) && value.to_str().is_ok()
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::Minted(n) => write!(f, "{:016x}{n:016x}", *PROCESS_SEED),
            // `is_usable` admitted only values `to_str` accepts.
            RequestId::Client(value) => f.write_str(value.to_str().unwrap_or_default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: HeaderName = HeaderName::from_static("x-request-id");

    fn headers(value: &'static [u8]) -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(HEADER, HeaderValue::from_bytes(value).unwrap());
        map
    }

    #[test]
    fn minted_ids_are_distinct_and_fixed_width() {
        let a = RequestId::mint();
        let b = RequestId::mint();
        assert_ne!(a, b);
        assert_ne!(a.to_string(), b.to_string());
        assert_eq!(a.to_string().len(), 32);
        assert!(a.to_string().bytes().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn minted_ids_of_two_threads_never_collide() {
        let other = std::thread::spawn(|| (0..1000).map(|_| RequestId::mint()).collect::<Vec<_>>())
            .join()
            .unwrap();
        let here: Vec<_> = (0..1000).map(|_| RequestId::mint()).collect();
        assert!(here.iter().all(|id| !other.contains(id)));
    }

    #[test]
    fn the_client_id_is_used_only_when_the_app_names_the_header() {
        let map = headers(b"abc-123");
        assert_eq!(RequestId::of(&map, Some(&HEADER)).to_string(), "abc-123");
        assert!(matches!(RequestId::of(&map, None), RequestId::Minted(_)));
    }

    #[test]
    fn an_unusable_client_id_is_replaced_by_a_minted_one() {
        let long = [b'a'; MAX_CLIENT_ID_LEN + 1];
        for value in [&b""[..], &b"caf\xc3\xa9"[..], &long[..]] {
            let mut map = HeaderMap::new();
            map.insert(HEADER, HeaderValue::from_bytes(value).unwrap());
            assert!(
                matches!(RequestId::of(&map, Some(&HEADER)), RequestId::Minted(_)),
                "{value:?}"
            );
        }
        let absent = HeaderMap::new();
        assert!(matches!(
            RequestId::of(&absent, Some(&HEADER)),
            RequestId::Minted(_)
        ));
    }
}
