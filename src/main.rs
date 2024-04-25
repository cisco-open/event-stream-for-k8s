use std::path::Path;
use std::pin::pin;
use std::time::{Duration, UNIX_EPOCH};

use futures::TryStreamExt;
use k8s_openapi::api::core::v1::Event;
use kube::runtime::watcher::{self, InitialListStrategy, ListSemantic};
use kube::runtime::WatchStreamExt;
use kube::Api;
use once_cell::sync::{Lazy, OnceCell};
use prometheus_exporter::prometheus::{
    opts, register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec,
};
use sled::Batch;
use tokio::sync::mpsc::Receiver;

mod config;

use config::CONFIG;

static PROM_EVENTS: Lazy<OnceCell<IntCounterVec>> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!("kube_event_stream_events_processed", "Events seen"),
        // COMBAK: or separate counters
        &["type"]
    )
    .unwrap()
    .into()
});
static PROM_BYTES: Lazy<OnceCell<IntCounter>> = Lazy::new(|| {
    register_int_counter!("kube_event_stream_bytes_synced", "Bytes synced to cache")
        .unwrap()
        .into()
});

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = get_db(&CONFIG.cache_db)?;

    let client = kube::Client::try_default().await?;

    let api: Api<Event> = Api::all(client);

    let watcher_config = watcher::Config {
        list_semantic: ListSemantic::Any,
        initial_list_strategy: InitialListStrategy::ListWatch,
        ..Default::default()
    };

    let stream = watcher::watcher(api, watcher_config)
        .default_backoff()
        .applied_objects();
    let mut stream = pin!(stream);

    // TODO: Is this queue size make sense?
    let (events_tx, events_rx) = tokio::sync::mpsc::channel::<Event>(1024);

    prometheus_exporter::start("0.0.0.0:9000".parse().unwrap())?;

    tokio::spawn(event_writer_task(db.clone(), events_rx));
    tokio::spawn(cache_cleanup_task(db.clone()));

    // TODO: Handle restarts/disconnects without crashing/exiting.
    while let Some(event) = stream.try_next().await? {
        events_tx.send(event).await?;
    }
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

async fn event_writer_task(db: sled::Db, mut events_rx: Receiver<Event>) {
    // TODO: Handle errros, be able to restart. Reduce .unwrap() usage.
    eprintln!("Starting event writer task...");
    loop {
        let mut events = vec![];
        let events_count = events_rx.recv_many(&mut events, 1024).await;
        if events_count == 0 {
            eprintln!("Shutting down event writer...");
            break;
        }
        let mut batch = Batch::default();
        let mut cache_hits: u64 = 0;
        let mut cache_misses: u64 = 0;
        for event in events {
            let key = format!(
                "{}:{}",
                event.metadata.uid.as_ref().unwrap_or(&String::default()),
                event
                    .metadata
                    .resource_version
                    .as_ref()
                    .unwrap_or(&String::default())
            );
            if db.contains_key(key.as_str()).unwrap() {
                cache_hits += 1;
                continue;
            }
            cache_misses += 1;

            println!("{}", serde_json::to_string(&event).unwrap());

            batch.insert(
                key.as_str(),
                &u64_to_u8_arr(UNIX_EPOCH.elapsed().unwrap().as_secs()),
            );
        }
        db.apply_batch(batch).unwrap();
        let bytes_synced = db.flush_async().await.unwrap();

        let prom_events = PROM_EVENTS.get().unwrap();
        prom_events
            .with_label_values(&["total"])
            .inc_by(events_count.try_into().unwrap());
        prom_events
            .with_label_values(&["cache_hits"])
            .inc_by(cache_hits);
        prom_events
            .with_label_values(&["cache_misses"])
            .inc_by(cache_misses);
        PROM_BYTES
            .get()
            .unwrap()
            .inc_by(bytes_synced.try_into().unwrap());

        eprintln!(
            "Processed {} events, out of which {} were already seen and {} were new. {} bytes synced to DB.",
            events_count, cache_hits, cache_misses, bytes_synced
        );
    }
}

async fn cache_cleanup_task(db: sled::Db) {
    // TODO: Handle errros, be able to restart. Reduce .unwrap() usage.
    eprintln!("Starting cache cleaner task...");
    loop {
        let now = UNIX_EPOCH.elapsed().unwrap().as_secs();
        let mut purged: usize = 0;
        while let Some(Ok((k, v))) = db.iter().next() {
            let ts = u8_slice_to_u64(v.as_ref());
            // eprintln!("now={} ts={} CACHE_TTL={}", now, ts, CACHE_TTL);
            if ts + CONFIG.cache_ttl < now {
                // eprintln!("{:?} is to be deleted", k);
                db.remove(k).unwrap();
                purged += 1;
            }
        }
        eprintln!(
            "Purged {} entries older than {} secs from the cache",
            purged, CONFIG.cache_ttl
        );
        if let Err(e) = db.flush_async().await {
            eprintln!("Error during fsync(): {:?}", e);
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
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
