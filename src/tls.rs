//! TLS support — builds a `tokio_rustls::TlsAcceptor` from PEM cert/key files.
//!
//! Opt-in via `app.run(tls_cert=..., tls_key=...)`. When either is None, the
//! server runs plain HTTP and this module is never invoked.
//!
//! Uses rustls with the `ring` crypto backend. ALPN advertises `h2` first,
//! then `http/1.1` — hyper's `AutoBuilder` picks the right protocol based on
//! the negotiated ALPN value.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

// Plain TCP or TLS-wrapped TCP — lets the accept loop hand hyper a single
// IO type regardless of whether TLS is configured. Both variants implement
// `AsyncRead + AsyncWrite` and delegate to the inner stream.
pin_project! {
    #[project = MaybeTlsProj]
    pub(crate) enum MaybeTlsStream {
        Plain { #[pin] inner: TcpStream },
        Tls { #[pin] inner: tokio_rustls::server::TlsStream<TcpStream> },
    }
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            MaybeTlsProj::Plain { inner } => inner.poll_read(cx, buf),
            MaybeTlsProj::Tls { inner } => inner.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.project() {
            MaybeTlsProj::Plain { inner } => inner.poll_write(cx, buf),
            MaybeTlsProj::Tls { inner } => inner.poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            MaybeTlsProj::Plain { inner } => inner.poll_flush(cx),
            MaybeTlsProj::Tls { inner } => inner.poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            MaybeTlsProj::Plain { inner } => inner.poll_shutdown(cx),
            MaybeTlsProj::Tls { inner } => inner.poll_shutdown(cx),
        }
    }
}

/// Why the TLS acceptor could not be built.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TlsError {
    #[error("open {what} {path:?}: {source}")]
    Open {
        what: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parse {what} {path:?}: {source}")]
    Parse {
        what: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("no certificates found in {0:?}")]
    NoCertificate(PathBuf),
    #[error("no private key found in {0:?}")]
    NoKey(PathBuf),
    #[error("TLS config error: {0}")]
    Config(#[from] rustls::Error),
}

/// Why a TLS handshake did not complete.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HandshakeError {
    #[error("TLS handshake error: {0}")]
    Failed(#[source] std::io::Error),
    #[error("TLS handshake timed out after {HANDSHAKE_TIMEOUT:?} (possible Slowloris)")]
    TimedOut,
}

/// How long a client gets to finish the TLS handshake.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

fn open(what: &'static str, path: &Path) -> Result<BufReader<File>, TlsError> {
    File::open(path)
        .map(BufReader::new)
        .map_err(|source| TlsError::Open {
            what,
            path: path.to_path_buf(),
            source,
        })
}

/// Load certificate chain (PEM) from disk.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let mut reader = open("cert", path)?;
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::Parse {
            what: "cert",
            path: path.to_path_buf(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificate(path.to_path_buf()));
    }
    Ok(certs)
}

/// Load a private key (PEM) from disk. Accepts PKCS#8, PKCS#1, or SEC1.
fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let mut reader = open("key", path)?;
    // private_key() picks the first key of any supported format.
    rustls_pemfile::private_key(&mut reader)
        .map_err(|source| TlsError::Parse {
            what: "key",
            path: path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| TlsError::NoKey(path.to_path_buf()))
}

/// Build a `TlsAcceptor` from cert/key PEM files. Called once at startup.
pub(crate) fn build_acceptor(
    cert_path: &str,
    key_path: &str,
) -> Result<Arc<TlsAcceptor>, TlsError> {
    // rustls needs a default crypto provider installed before any ServerConfig
    // is built. Idempotent — `install_default` errors if already installed, so
    // we ignore the result.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let certs = load_certs(Path::new(cert_path))?;
    let key = load_key(Path::new(key_path))?;

    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    // Advertise HTTP/2 and HTTP/1.1 via ALPN. hyper_util::server::conn::auto
    // selects the right protocol based on the negotiated ALPN value.
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(Arc::new(TlsAcceptor::from(Arc::new(cfg))))
}

/// Perform the TLS handshake and wrap the result in `MaybeTlsStream::Tls`.
///
/// Bounded by [`HANDSHAKE_TIMEOUT`] — defense against TLS
/// Slowloris attacks where a peer opens a TCP connection then dribbles
/// ClientHello bytes one per 30 s, pinning a file descriptor and an
/// async task indefinitely. With 65k half-open connections a single
/// laptop can exhaust the server's fd budget without ever finishing
/// a handshake; the timeout closes the loop.
async fn wrap_tls(
    acceptor: &TlsAcceptor,
    stream: TcpStream,
) -> Result<MaybeTlsStream, HandshakeError> {
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
        Ok(Ok(tls)) => Ok(MaybeTlsStream::Tls { inner: tls }),
        Ok(Err(e)) => Err(HandshakeError::Failed(e)),
        Err(_) => Err(HandshakeError::TimedOut),
    }
}

/// An accepted connection as the stream to serve: TLS-handshaken when its listener has
/// an acceptor, plain otherwise. A failed handshake is logged here and gives `None` (the
/// connection is dropped).
pub(crate) async fn wrap(stream: TcpStream, tls: Option<&TlsAcceptor>) -> Option<MaybeTlsStream> {
    match tls {
        None => Some(MaybeTlsStream::Plain { inner: stream }),
        Some(acceptor) => match wrap_tls(acceptor, stream).await {
            Ok(stream) => Some(stream),
            Err(e) => {
                tracing::warn!(target: "pyronova::server", error = %e, "TLS handshake failed");
                None
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_cert_is_a_typed_open_error_naming_the_file() {
        let err = build_acceptor("/nonexistent/m4-cert.pem", "/nonexistent/m4-key.pem")
            .err()
            .expect("no such file");
        match &err {
            TlsError::Open { what, path, source } => {
                assert_eq!(*what, "cert");
                assert_eq!(path, Path::new("/nonexistent/m4-cert.pem"));
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected TlsError::Open, got {other:?}"),
        }
        assert!(err.to_string().contains("m4-cert.pem"), "{err}");
    }

    #[test]
    fn an_empty_cert_file_says_so() {
        let dir = std::env::temp_dir().join(format!("pyronova-m4-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("empty.pem");
        std::fs::write(&cert, b"").unwrap();
        let err = build_acceptor(cert.to_str().unwrap(), "/nonexistent/key.pem")
            .err()
            .expect("no certificate");
        assert!(
            matches!(err, TlsError::NoCertificate(ref p) if p == &cert),
            "{err:?}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
