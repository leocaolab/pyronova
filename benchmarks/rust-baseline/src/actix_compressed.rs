//! Actix-web compression baseline for head-to-head comparison with Pyronova.
//!
//! Same /json-fortunes payload (32 records, ~3 KB JSON), same stack, same
//! machine, same wrk invocation as benchmarks/bench_compression.py. Uses
//! actix-web's default `Compress` middleware which negotiates brotli / gzip /
//! deflate / zstd per the client's Accept-Encoding.
//!
//! Build:
//!     cd benchmarks/rust-baseline && cargo build --release --bin bench-actix-compressed
//!
//! Run:
//!     ./target/release/bench-actix-compressed   # listens on 127.0.0.1:8002

use actix_web::{get, middleware::Compress, App, HttpResponse, HttpServer};
use serde::Serialize;

#[derive(Serialize)]
struct Fortune {
    id: i32,
    message: &'static str,
}

#[derive(Serialize)]
struct FortunesResponse {
    fortunes: Vec<Fortune>,
}

// Same 32 fortunes as benchmarks/bench_compression.py — varied English so
// compression sees real redundancy (not degenerate repetition). Typed structs
// (not an anonymous serde_json::Value) give compile-time field safety;
// serialization is deferred to the HTTP response boundary in json_fortunes().
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

#[get("/")]
async fn index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain")
        .body("Hello from Actix-web!")
}

#[get("/json-fortunes")]
async fn json_fortunes() -> HttpResponse {
    HttpResponse::Ok().json(fortunes())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = std::env::var("ACTIX_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8002);
    println!(
        "\n  Actix-web compressed baseline listening on http://127.0.0.1:{port}\n"
    );

    HttpServer::new(|| {
        App::new()
            // Default Compress: negotiates Accept-Encoding (br/gzip/deflate/zstd).
            // Same as Pyronova — handler returns plain JSON, middleware layers encoding on top.
            .wrap(Compress::default())
            .service(index)
            .service(json_fortunes)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
