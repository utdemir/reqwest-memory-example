//! ureq streaming variant of the load harness. Standalone — shares no code with
//! the other binaries. Instead of reqwest's async client, this uses `ureq`, a
//! synchronous (blocking) HTTP client, driven from the Tokio runtime via
//! `spawn_blocking`.
//!
//! Each of the WORKERS async tasks loops, and on every iteration offloads one
//! blocking ureq request onto Tokio's blocking thread pool with
//! `tokio::task::spawn_blocking`. A single `ureq::Agent` is shared across all of
//! them (it is Arc-backed and cheap to clone, so all clones reuse one connection
//! pool). The response body is streamed to a sink so a whole `.ts` segment is
//! never buffered in memory — the only difference from `ureq_collecting` is this
//! body handling.
//!
//! Run with: `cargo run --release --bin ureq_streaming`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Number of concurrent workers (each drives one in-flight blocking request).
const WORKERS: usize = 16;

/// The target URL every worker requests in a loop.
const URL: &str =
    "https://dd6g9dllgm9fw.cloudfront.net/Parkour-loop-mp4/out_1/00000/out_1_00001.ts";

/// How often the memory reporter prints a line.
const REPORT_INTERVAL: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    // A single ureq agent shared across all workers. Cloning is cheap (internal
    // Arc) and all clones share one connection pool.
    let agent: ureq::Agent = ureq::Agent::config_builder().build().into();

    let requests = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(WORKERS);
    for id in 0..WORKERS {
        let agent = agent.clone();
        let requests = Arc::clone(&requests);
        handles.push(tokio::spawn(async move {
            worker(id, agent, requests).await;
        }));
    }

    tokio::spawn(reporter(Arc::clone(&requests)));

    println!(
        "[ureq_streaming] Started {WORKERS} workers against {URL}\nReporting memory every {}s. Press Ctrl-C to stop.",
        REPORT_INTERVAL.as_secs()
    );

    for handle in handles {
        let _ = handle.await;
    }
}

/// A single worker: forever offloads one blocking ureq request onto the blocking
/// thread pool, awaiting each before issuing the next.
async fn worker(id: usize, agent: ureq::Agent, requests: Arc<AtomicU64>) {
    loop {
        let agent = agent.clone();
        // spawn_blocking runs the synchronous ureq call on Tokio's dedicated
        // blocking thread pool, keeping the async worker free to await it.
        let result = tokio::task::spawn_blocking(move || blocking_request(&agent)).await;

        match result {
            Ok(Ok(())) => {
                requests.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(e)) => {
                eprintln!("[worker {id}] request error: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                // spawn_blocking join error (panic in the blocking closure).
                eprintln!("[worker {id}] join error: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Performs one synchronous request and streams the body to a sink so it is
/// never fully buffered. Runs on a blocking thread.
fn blocking_request(agent: &ureq::Agent) -> Result<(), String> {
    let mut response = agent.get(URL).call().map_err(|e| e.to_string())?;
    let mut reader = response.body_mut().as_reader();
    std::io::copy(&mut reader, &mut std::io::sink()).map_err(|e| e.to_string())?;
    Ok(())
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
                    "[ureq_streaming] mem: phys {phys_mb:8.2} MB | virt {virt_mb:8.2} MB | requests: {total} total, {per_sec:.1}/s"
                );
            }
            None => {
                println!("[ureq_streaming] mem: <unavailable> | requests: {total} total, {per_sec:.1}/s");
            }
        }
    }
}
