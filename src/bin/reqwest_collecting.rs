//! Collecting variant of the load harness. Standalone — shares no code with the
//! other binaries. Identical to `reqwest_streaming` (same default `reqwest::Client`)
//! except each response body is fully buffered into memory with `.bytes()` rather
//! than streamed and discarded chunk-by-chunk. The whole `.ts` segment is therefore
//! resident at once, per concurrent worker — the only difference from
//! `reqwest_streaming` is this body handling.
//!
//! Run with: `cargo run --release --bin reqwest_collecting`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Number of concurrent workers (Tokio tasks) hammering the URL.
/// Bump this up/down to change the load.
const WORKERS: usize = 16;

/// The target URL every worker requests in a loop.
const URL: &str =
    "https://dd6g9dllgm9fw.cloudfront.net/Parkour-loop-mp4/out_1/00000/out_1_00001.ts";

/// How often the memory reporter prints a line.
const REPORT_INTERVAL: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    // A single client, shared across all workers. reqwest::Client is internally
    // Arc-backed, so cloning it is cheap and reuses the same connection pool.
    let client = reqwest::Client::builder()
        .build()
        .expect("failed to build reqwest client");

    // Total successful requests, shared with the reporter for a req/s readout.
    let requests = Arc::new(AtomicU64::new(0));

    // Spawn the worker tasks.
    let mut handles = Vec::with_capacity(WORKERS);
    for id in 0..WORKERS {
        let client = client.clone();
        let requests = Arc::clone(&requests);
        handles.push(tokio::spawn(async move {
            worker(id, client, requests).await;
        }));
    }

    // Spawn the memory reporter.
    tokio::spawn(reporter(Arc::clone(&requests)));

    println!(
        "[reqwest_collecting] Started {WORKERS} workers against {URL}\nReporting memory every {}s. Press Ctrl-C to stop.",
        REPORT_INTERVAL.as_secs()
    );

    // Run until the process is killed (Ctrl-C). The workers loop forever.
    for handle in handles {
        let _ = handle.await;
    }
}

/// A single worker: requests the URL forever, buffering each entire response body
/// into memory with `.bytes()` before dropping it.
async fn worker(id: usize, client: reqwest::Client, requests: Arc<AtomicU64>) {
    loop {
        match client.get(URL).send().await {
            Ok(resp) => {
                // Collect the full body into memory — the whole segment is
                // resident at once before it is dropped here.
                match resp.bytes().await {
                    Ok(_bytes) => {
                        requests.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("[worker {id}] body error: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
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
    // First tick fires immediately; skip it so the first report covers a full interval.
    ticker.tick().await;

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
                    "[reqwest_collecting] mem: phys {phys_mb:8.2} MB | virt {virt_mb:8.2} MB | requests: {total} total, {per_sec:.1}/s"
                );
            }
            None => {
                println!("[reqwest_collecting] mem: <unavailable> | requests: {total} total, {per_sec:.1}/s");
            }
        }
    }
}
