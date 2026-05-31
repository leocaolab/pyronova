//! Actix-web — TUNED compression for fair head-to-head with Pyronova.
//!
//! Differences vs actix_compressed.rs (which uses default `Compress` middleware):
//!   * Brotli quality 4 (Pyronova's default), not Actix's default quality 11
//!   * Gzip level 6 (Pyronova's default, also flate2 default)
//!   * Compression runs on `web::block` (Actix's spawn_blocking equivalent)
//!     so it doesn't head-of-line block the async runtime — matches Pyronova's
//!     `tokio::task::spawn_blocking` strategy
//!   * Same crates Pyronova uses: `brotli` 7.x + `flate2` 1.x
//!
//! This is what an experienced Actix user would write if they needed fast
//! compression. It's the fair comparison — same CPU work on both sides,
//! both frameworks using their preferred runtime affinity primitives.

use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer};
use serde::Serialize;
use std::io::Write;

const BROTLI_QUALITY: i32 = 4;
const GZIP_LEVEL: u32 = 6;
const MIN_COMPRESS_SIZE: usize = 256;

#[derive(Serialize)]
struct Fortune {
    id: u32,
    message: &'static str,
}

#[derive(Serialize)]
struct FortunesResponse {
    fortunes: Vec<Fortune>,
}

fn fortunes() -> FortunesResponse {
    FortunesResponse {
        fortunes: vec![
            Fortune { id: 1, message: "fortune: No such file or directory" },
            Fortune { id: 2, message: "A computer scientist is someone who fixes things that aren't broken." },
            Fortune { id: 3, message: "After enough decimal places, nobody gives a damn." },
            Fortune { id: 4, message: "A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1" },
            Fortune { id: 5, message: "A computer program does what you tell it to do, not what you want it to do." },
            Fortune { id: 6, message: "Emacs is a nice operating system, but I prefer UNIX. — Tom Christaensen" },
            Fortune { id: 7, message: "Any program that runs right is obsolete." },
            Fortune { id: 8, message: "A list is only as strong as its weakest link. — Donald Knuth" },
            Fortune { id: 9, message: "Feature: A bug with seniority." },
            Fortune { id: 10, message: "Computers make very fast, very accurate mistakes." },
            Fortune { id: 11, message: "<script>alert(\"This should not be displayed in a browser alert box.\");</script>" },
            Fortune { id: 12, message: "フレームワークのベンチマーク" },
            Fortune { id: 13, message: "Additional fortune added at request time." },
            Fortune { id: 14, message: "Good programmers have a solid grasp of their tools." },
            Fortune { id: 15, message: "The only constant is change." },
            Fortune { id: 16, message: "Premature optimization is the root of all evil. — Donald Knuth" },
            Fortune { id: 17, message: "There are only two hard things in Computer Science: cache invalidation and naming things." },
            Fortune { id: 18, message: "Testing shows the presence, not the absence of bugs. — Edsger Dijkstra" },
            Fortune { id: 19, message: "Simplicity is prerequisite for reliability. — Edsger Dijkstra" },
            Fortune { id: 20, message: "When in doubt, use brute force. — Ken Thompson" },
            Fortune { id: 21, message: "Controlling complexity is the essence of computer programming. — Brian Kernighan" },
            Fortune { id: 22, message: "The most important property of a program is whether it accomplishes the intention of its user." },
            Fortune { id: 23, message: "Measuring programming progress by lines of code is like measuring aircraft building progress by weight." },
            Fortune { id: 24, message: "The best performance improvement is the transition from the nonworking state to the working state." },
            Fortune { id: 25, message: "Deleted code is debugged code. — Jeff Sickel" },
            Fortune { id: 26, message: "First, solve the problem. Then, write the code. — John Johnson" },
            Fortune { id: 27, message: "Programs must be written for people to read, and only incidentally for machines to execute." },
            Fortune { id: 28, message: "Any sufficiently advanced bug is indistinguishable from a feature." },
            Fortune { id: 29, message: "There's no place like 127.0.0.1." },
            Fortune { id: 30, message: "It is practically impossible to teach good programming to students who have had a prior exposure to BASIC." },
            Fortune { id: 31, message: "Walking on water and developing software from a specification are easy if both are frozen." },
            Fortune { id: 32, message: "Debugging is twice as hard as writing the code in the first place." },
        ],
    }
}

fn negotiate(accept_encoding: &str) -> Option<&'static str> {
    // Match Pyronova: prefer brotli > gzip. Ignore q-values for simplicity (the
    // wrk test sends no q-values anyway; Pyronova's full q-parser is tested elsewhere).
    if accept_encoding.split(',').any(|e| e.trim().eq_ignore_ascii_case("br")) {
        Some("br")
    } else if accept_encoding
        .split(',')
        .any(|e| e.trim().eq_ignore_ascii_case("gzip"))
    {
        Some("gzip")
    } else {
        None
    }
}

fn compress_sync(body: &[u8], algo: &str) -> Vec<u8> {
    match algo {
        "br" => {
            let mut out = Vec::with_capacity(body.len() / 2);
            let params = brotli::enc::BrotliEncoderParams {
                quality: BROTLI_QUALITY,
                ..Default::default()
            };
            let mut reader = body;
            brotli::BrotliCompress(&mut reader, &mut out, &params).unwrap();
            out
        }
        "gzip" => {
            let mut enc = flate2::write::GzEncoder::new(
                Vec::with_capacity(body.len() / 2),
                flate2::Compression::new(GZIP_LEVEL),
            );
            enc.write_all(body).unwrap();
            enc.finish().unwrap()
        }
        _ => body.to_vec(),
    }
}

#[get("/")]
async fn index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain")
        .body("Hello from Actix-web (tuned)!")
}

#[get("/json-fortunes")]
async fn json_fortunes(req: HttpRequest) -> Result<HttpResponse, actix_web::Error> {
    // Serialize once, then maybe compress off the async runtime.
    let payload = serde_json::to_vec(&fortunes()).unwrap();

    let accept = req
        .headers()
        .get("accept-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if payload.len() < MIN_COMPRESS_SIZE {
        return Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(payload));
    }

    match negotiate(&accept) {
        Some(algo) => {
            let algo_static = algo; // &'static str, no allocation
            // web::block puts the compression on Actix's blocking thread pool —
            // matches Pyronova's tokio::task::spawn_blocking. Prevents HoL blocking
            // of the async worker.
            let compressed = web::block(move || compress_sync(&payload, algo_static))
                .await
                .map_err(actix_web::error::ErrorInternalServerError)?;
            Ok(HttpResponse::Ok()
                .insert_header(("content-encoding", algo_static))
                .insert_header(("vary", "Accept-Encoding"))
                .content_type("application/json")
                .body(compressed))
        }
        None => Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(payload)),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = std::env::var("ACTIX_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8002);
    println!(
        "\n  Actix-web TUNED compressed baseline listening on http://127.0.0.1:{port}\n  (brotli q={BROTLI_QUALITY}, gzip level={GZIP_LEVEL}, web::block off-runtime)\n"
    );

    HttpServer::new(|| {
        App::new()
            // Logger-free to match Pyronova's bench config (log disabled).
            .wrap(middleware::DefaultHeaders::new())
            .service(index)
            .service(json_fortunes)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
