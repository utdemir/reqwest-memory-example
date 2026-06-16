//! Tweaks variant of the load harness. Standalone — shares no code with the other
//! binaries. Identical to `reqwest_streaming` except the `reqwest::Client` is built
//! with memory-conscious flags: no retained idle connections, a short idle timeout,
//! and small, fixed HTTP/2 flow-control windows (adaptive growth disabled).
//!
//! Response bodies are streamed and discarded chunk-by-chunk, the same as
//! `reqwest_streaming`, so the only difference between the two is the client config.
//!
//! Run with: `cargo run --release --bin reqwest_tweaks`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;

/// Number of concurrent workers (Tokio tasks) hammering the URL.
const WORKERS: usize = 16;

/// The target URL every worker requests in a loop.
const URL: &str =
    "https://dd6g9dllgm9fw.cloudfront.net/Parkour-loop-mp4/out_1/00000/out_1_00001.ts";

/// How often the memory reporter prints a line.
const REPORT_INTERVAL: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    // A single client shared across all workers, tuned for low memory:
    //   - pool_max_idle_per_host(0): never retain idle connections (each idle
    //     socket holds TLS state + buffers). Default is unlimited.
    //   - pool_idle_timeout: drop any idle socket quickly.
    //   - small fixed HTTP/2 windows + adaptive disabled: cap how much body
    //     data hyper buffers in flight per stream/connection.
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .pool_idle_timeout(Duration::from_secs(5))
        .http2_initial_stream_window_size(64 * 1024)
        .http2_initial_connection_window_size(64 * 1024)
        .http2_adaptive_window(false)
        .build()
        .expect("failed to build reqwest client");

    let requests = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(WORKERS);
    for id in 0..WORKERS {
        let client = client.clone();
        let requests = Arc::clone(&requests);
        handles.push(tokio::spawn(async move {
            worker(id, client, requests).await;
        }));
    }

    tokio::spawn(reporter(Arc::clone(&requests)));

    println!(
        "[reqwest_tweaks] Started {WORKERS} workers against {URL}\nReporting memory every {}s. Press Ctrl-C to stop.",
        REPORT_INTERVAL.as_secs()
    );

    for handle in handles {
        let _ = handle.await;
    }
}

/// A single worker: requests the URL forever, streaming each response body and
/// discarding it chunk-by-chunk so a whole segment is never held in memory.
async fn worker(id: usize, client: reqwest::Client, requests: Arc<AtomicU64>) {
    loop {
        match client.get(URL).send().await {
            Ok(resp) => {
                let mut stream = resp.bytes_stream();
                let mut ok = true;
                while let Some(chunk) = stream.next().await {
                    if let Err(e) = chunk {
                        eprintln!("[worker {id}] body error: {e}");
                        ok = false;
                        break;
                    }
                    // chunk drops here — only one chunk resident at a time.
                }
                if ok {
                    requests.fetch_add(1, Ordering::Relaxed);
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Err(e) => {
                eprintln!("[worker {id}] request error: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Prints current-process memory usage and throughput on a fixed interval.
async fn reporter(requests: Arc<AtomicU64>) {
    let mut last_count = 0u64;
    let mut ticker = tokio::time::interval(REPORT_INTERVAL);
    ticker.tick().await; // skip the immediate first tick

    loop {
        ticker.tick().await;

        let total = requests.load(Ordering::Relaxed);
        let delta = total - last_count;
        last_count = total;
        let per_sec = delta as f64 / REPORT_INTERVAL.as_secs_f64();

        match memory_stats::memory_stats() {
            Some(usage) => {
                let phys_mb = usage.physical_mem as f64 / (1024.0 * 1024.0);
                let virt_mb = usage.virtual_mem as f64 / (1024.0 * 1024.0);
                println!(
                    "[reqwest_tweaks] mem: phys {phys_mb:8.2} MB | virt {virt_mb:8.2} MB | requests: {total} total, {per_sec:.1}/s"
                );
            }
            None => {
                println!("[reqwest_tweaks] mem: <unavailable> | requests: {total} total, {per_sec:.1}/s");
            }
        }
    }
}
