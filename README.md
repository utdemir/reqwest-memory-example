# reqwest-memory-example

A small load-test harness for comparing the memory footprint of different HTTP
client setups in Rust. Each variant is a standalone binary that hammers a single
URL with a fixed number of concurrent workers — all sharing **one** reused
client — while printing the process's resident memory every 5 seconds.

The point is to see what actually drives memory under sustained load: it turns
out **how you consume the response body matters far more than how you configure
the client**.

## Variants

All four use `WORKERS = 16` concurrent workers and report memory every 5s.

| Binary | Client | Body handling |
|---|---|---|
| `reqwest_streaming`  | default `reqwest::Client` | streamed and dropped chunk-by-chunk (`bytes_stream()`) |
| `reqwest_collecting` | default `reqwest::Client` | whole body buffered into memory (`.bytes()`) |
| `reqwest_tweaks`     | reqwest with memory-conscious flags | streamed and dropped chunk-by-chunk |
| `ureq_blocking`      | `ureq` (blocking) via Tokio `spawn_blocking` | streamed to a sink |

The "memory-conscious flags" in `reqwest_tweaks`:

```rust
reqwest::Client::builder()
    .pool_max_idle_per_host(0)                          // don't retain idle connections
    .pool_idle_timeout(Duration::from_secs(5))
    .http2_initial_stream_window_size(64 * 1024)        // small fixed flow-control windows
    .http2_initial_connection_window_size(64 * 1024)
    .http2_adaptive_window(false)                       // don't grow windows under load
    .build()
```

## Running

```sh
cargo run --release --bin reqwest_streaming
cargo run --release --bin reqwest_collecting
cargo run --release --bin reqwest_tweaks
cargo run --release --bin ureq_blocking
```

Each runs until you stop it with Ctrl-C. Tune the load by editing the `WORKERS`
constant at the top of the binary.

## Results

Steady-state resident memory (RSS) after warm-up, 16 workers against a CloudFront
`.ts` segment, ~30s per run on macOS (Apple Silicon):

| Variant | Steady RSS | Throughput |
|---|---:|---:|
| `reqwest_streaming`  | **~16 MB** | ~42 req/s |
| `reqwest_collecting` | ~40 MB     | ~42 req/s |
| `reqwest_tweaks`     | **~15 MB** | ~34 req/s |
| `ureq_blocking`      | ~26 MB     | ~43 req/s |

### Takeaways

- **Body buffering dominates.** `reqwest_collecting` (~40 MB) vs.
  `reqwest_streaming` (~16 MB) is the same client — the only difference is
  buffering the whole segment with `.bytes()` instead of streaming it. That one
  change is a ~2.5× memory difference.
- **Client flags barely matter once you stream.** `reqwest_tweaks` (~15 MB) is
  essentially identical to `reqwest_streaming` (~16 MB). The pool/window tuning
  doesn't move the needle here (and costs ~20% throughput, likely from the small
  HTTP/2 windows throttling each download). The flags would matter more on
  workloads with many idle connections or large in-flight windows.
- **`ureq` sits in the middle** (~26 MB) at full throughput — a leaner blocking
  client with no HTTP/2 framing/flow-control machinery, driven from Tokio's
  blocking thread pool.

> Note: the large *virtual* memory figure printed by each binary (~415 GB) is
> just reserved address space (allocator arenas / thread guard pages), not real
> usage. **Physical / RSS is the number that matters.**

## How it works

Each binary spawns `WORKERS` Tokio tasks looping requests against one shared
client (reqwest's `Client` and ureq's `Agent` are both `Arc`-backed, so cloning
reuses the same connection pool). A separate reporter task prints memory via the
[`memory-stats`](https://crates.io/crates/memory-stats) crate every 5 seconds,
alongside total requests and requests/sec.
