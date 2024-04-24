use std::pin::pin;
use std::time::{Duration, UNIX_EPOCH};

use futures::TryStreamExt;
use k8s_openapi::api::core::v1::Event;
use kube::runtime::watcher::{self, InitialListStrategy, ListSemantic};
use kube::runtime::WatchStreamExt;
use kube::Api;
use sled::Batch;
use tokio::sync::mpsc::Receiver;

static CACHE_TTL: u64 = 3600;

// TODO: Metrics

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    // TODO: Handle corrupt DB
    let db = sled::open("k8s-events-bookmarks")?;

    // TODO: Is this queue size make sense?
    let (events_tx, events_rx) = tokio::sync::mpsc::channel::<Event>(1024);

    tokio::spawn(event_writer_task(db.clone(), events_rx));
    tokio::spawn(cache_cleanup_task(db.clone()));

    // TODO: Handle restarts/disconnects without crashing/exiting.
    while let Some(event) = stream.try_next().await? {
        events_tx.send(event).await?;
    }
    Ok(())
}

async fn event_writer_task(db: sled::Db, mut events_rx: Receiver<Event>) {
    // TODO: Handle errros, be able to restart. Reduce .unwrap() usage.
    eprintln!("Starting event writer task...");
    /*
    K - 11
    u - 21
    b -  2
    e -  5
    r - 18
    n - 14
    e -  5
    t - 20
    e -  5
    s - 19
    ------
       120
    */
    let hasher = ahash::RandomState::with_seeds(120, 4, 23, 2024);
    loop {
        let mut events = vec![];
        events_rx.recv_many(&mut events, 1024).await;
        let mut batch = Batch::default();
        let events_count = events.len();
        let mut cache_hits: usize = 0;
        let mut cache_misses: usize = 0;
        for event in events {
            let json_event = serde_json::to_string(&event).unwrap();
            let event_hash = u64_to_u8_arr(hasher.hash_one(&json_event));
            if db.contains_key(event_hash).unwrap() {
                cache_hits += 1;
                continue;
            }
            cache_misses += 1;
            println!("{}", json_event);
            batch.insert(
                &event_hash,
                &u64_to_u8_arr(UNIX_EPOCH.elapsed().unwrap().as_secs()),
            );
        }
        db.apply_batch(batch).unwrap();
        let bytes_synced = db.flush_async().await.unwrap();
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
            if ts + CACHE_TTL < now {
                // eprintln!("{:?} is to be deleted", k);
                db.remove(k).unwrap();
                purged += 1;
            }
        }
        eprintln!(
            "Purged {} entries older than {} secs from the cache",
            purged, CACHE_TTL
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
