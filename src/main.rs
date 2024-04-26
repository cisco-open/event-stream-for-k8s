use std::path::Path;
use std::pin::pin;
use std::time::{Duration, UNIX_EPOCH};

use futures::future::{select, select_all, Either};
use futures::StreamExt;
use k8s_openapi::api::core::v1::Event;
use k8s_openapi::serde::Serialize;
use kube::runtime::watcher::{self, InitialListStrategy, ListSemantic};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client};
use once_cell::sync::{Lazy, OnceCell};
use prometheus_exporter::prometheus::{
    opts, register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec,
};
use sled::Batch;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender};

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

    // TODO: Is this queue size make sense?
    let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<Event>(1024);

    let _exporter = prometheus_exporter::start("0.0.0.0:9000".parse()?)?;

    let (term_tx, term_rx) = tokio::sync::broadcast::channel::<()>(1);

    let writer = tokio::spawn(event_writer_task(db.clone(), ev_rx, term_rx.resubscribe()));
    let cleaner = tokio::spawn(cache_cleanup_task(db.clone(), term_rx.resubscribe()));
    let watcher = tokio::spawn(event_watcher_task(client, ev_tx, term_rx.resubscribe()));
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

#[derive(Debug, Serialize)]
struct KubernetesEvent {
    kubernetes_event: Event,
}

#[derive(thiserror::Error, Debug)]
enum KesError {
    #[error(transparent)]
    ChannelSend(#[from] SendError<Event>),
    #[error(transparent)]
    Watcher(#[from] kube::runtime::watcher::Error),
    #[error(transparent)]
    Database(#[from] sled::Error),
    #[error(transparent)]
    EventSerialization(#[from] serde_json::Error),
    #[error(transparent)]
    SystemTime(#[from] std::time::SystemTimeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
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

async fn event_watcher_task(
    client: Client,
    events_tx: Sender<Event>,
    mut term_rx: broadcast::Receiver<()>,
) -> Result<(), KesError> {
    eprintln!("Starting event watcher task...");

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

    loop {
        match select(stream.next(), pin!(term_rx.recv())).await {
            Either::Left((Some(Ok(event)), _)) => events_tx.send(event).await?,
            Either::Left((Some(Err(e)), _)) => {
                eprintln!("Error receiving events: {:?}", e);
            }
            Either::Left((None, _)) | Either::Right(_) => {
                eprintln!("Stopping event watcher task!");
                return Ok(());
            }
        }
    }
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

async fn event_writer_task(
    db: sled::Db,
    mut events_rx: Receiver<Event>,
    mut term_rx: broadcast::Receiver<()>,
) -> Result<(), KesError> {
    // TODO: Handle errros, be able to restart. Reduce .unwrap() usage.
    eprintln!("Starting event writer task...");
    loop {
        let mut events = vec![];

        let events_count = match select(
            pin!(events_rx.recv_many(&mut events, 1024)),
            pin!(term_rx.recv()),
        )
        .await
        {
            Either::Left((0, _)) => {
                eprintln!("Event channel dropped, stopping event writer task!");
                return Ok(());
            }
            Either::Left((c, _)) => c,
            Either::Right(_) => {
                eprintln!("Stopping event writer task!");
                return Ok(());
            }
        };

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
            if db.contains_key(key.as_str())? {
                cache_hits += 1;
                continue;
            }
            cache_misses += 1;

            println!(
                "{}",
                serde_json::to_string(&KubernetesEvent {
                    kubernetes_event: event
                })?
            );

            batch.insert(
                key.as_str(),
                &u64_to_u8_arr(UNIX_EPOCH.elapsed()?.as_secs()),
            );
        }
        db.apply_batch(batch)?;
        let bytes_synced = db.flush_async().await?;

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

async fn cache_cleanup_task(
    db: sled::Db,
    mut term_rx: broadcast::Receiver<()>,
) -> Result<(), KesError> {
    eprintln!("Starting cache cleaner task...");
    loop {
        let now = UNIX_EPOCH.elapsed()?.as_secs();
        let mut purged: usize = 0;

        for item in db.iter() {
            let (k, v) = item?;
            let ts = u8_slice_to_u64(v.as_ref());
            if ts + CONFIG.cache_ttl < now {
                db.remove(k)?;
                purged += 1;
            }
        }

        if purged > 0 {
            eprintln!(
                "Purged {} entries older than {} secs from the cache",
                purged, CONFIG.cache_ttl
            );
            db.flush_async().await?;
        }

        // This is too frequent, but for now we want to test if deadlocks will happen
        if let Either::Right(_) = select(
            pin!(tokio::time::sleep(Duration::from_secs(5))),
            pin!(term_rx.recv()),
        )
        .await
        {
            eprintln!("Stopping cache cleaner task!");
            return Ok(());
        };
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
