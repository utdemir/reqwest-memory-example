//! Streaming variant: continuously hammers a single URL with a configurable number
//! of concurrent workers, all sharing one default `reqwest::Client`, while printing
//! the process memory usage every few seconds. Useful for observing whether the
//! HTTP client's memory footprint grows over time under sustained load.
//!
//! Response bodies are streamed and discarded chunk-by-chunk. The only difference
//! from `reqwest_collecting` is the body handling (stream vs. buffer the whole
//! segment); the only difference from `reqwest_tweaks` is the client configuration.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;

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
        "[reqwest_streaming] Started {WORKERS} workers against {URL}\nReporting memory every {}s. Press Ctrl-C to stop.",
        REPORT_INTERVAL.as_secs()
    );

    // Run until the process is killed (Ctrl-C). The workers loop forever.
    for handle in handles {
        let _ = handle.await;
    }
}

/// A single worker: requests the URL forever, streaming each response body and
/// discarding it chunk-by-chunk so a whole segment is never held in memory.
/// (Body handling is identical across the variants; only the client config differs.)
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
                    "[reqwest_streaming] mem: phys {phys_mb:8.2} MB | virt {virt_mb:8.2} MB | requests: {total} total, {per_sec:.1}/s"
                );
            }
            None => {
                println!("[reqwest_streaming] mem: <unavailable> | requests: {total} total, {per_sec:.1}/s");
            }
        }
    }
}
