use std::path::Path;
use std::time::Duration;

use futures::future::{select, select_all};
use k8s_openapi::api::core::v1::Event;
use tokio::signal::unix::{signal, SignalKind};

mod config;
mod tasks;
mod types;

use config::CONFIG;
use tasks::{clean_cache, watch_events, write_events};
use types::KesError;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = get_db(&CONFIG.cache_db)?;

    let client = kube::Client::try_default().await?;

    // TODO: Is this queue size make sense?
    let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<Event>(1024);

    let _exporter = prometheus_exporter::start("0.0.0.0:9000".parse()?)?;

    let (term_tx, term_rx) = tokio::sync::broadcast::channel::<()>(1);

    let writer = tokio::spawn(write_events(db.clone(), ev_rx, term_rx.resubscribe()));
    let cleaner = tokio::spawn(clean_cache(db.clone(), term_rx.resubscribe()));
    let watcher = tokio::spawn(watch_events(client, ev_tx, term_rx.resubscribe()));
    let term_req = tokio::spawn(term_request());

    // Wait for any task to complete.
    let _ = select_all(vec![writer, cleaner, watcher, term_req]).await;

    // Broadcast the shutdown signal to all tasks.
    term_tx.send(())?;

    // Give the tasks a chance to notice and stop.
    tokio::time::sleep(Duration::from_secs(1)).await;

    eprintln!("Bye!");

    Ok(())
}

async fn term_request() -> Result<(), KesError> {
    select(
        Box::pin(signal(SignalKind::interrupt())?.recv()),
        Box::pin(signal(SignalKind::terminate())?.recv()),
    )
    .await;
    eprintln!("User initiatied shutdown started!");
    Ok(())
}

fn get_db(path: &Path) -> Result<sled::Db, sled::Error> {
    match sled::open(path) {
        Ok(db) => Ok(db),
        Err(sled::Error::Corruption { .. } | sled::Error::Unsupported(_)) => {
            std::fs::remove_dir_all(path)?;
            eprintln!("DB corrupt; recreating it");
            get_db(path)
        }
        Err(e) => Err(e),
    }
}

#[inline]
fn u64_to_u8_arr(x: u64) -> [u8; 8] {
    [
        (x >> 56) as u8,
        (x >> 48) as u8,
        (x >> 40) as u8,
        (x >> 32) as u8,
        (x >> 24) as u8,
        (x >> 16) as u8,
        (x >> 8) as u8,
        x as u8,
    ]
}

#[inline]
fn u8_slice_to_u64(arr: &[u8]) -> u64 {
    if arr.len() != 8 {
        panic!("The u8 slice needs to be 8 bytes long");
    }
    (u64::from(arr[0]) << 56)
        | (u64::from(arr[1]) << 48)
        | (u64::from(arr[2]) << 40)
        | (u64::from(arr[3]) << 32)
        | (u64::from(arr[4]) << 24)
        | (u64::from(arr[5]) << 16)
        | (u64::from(arr[6]) << 8)
        | u64::from(arr[7])
}
