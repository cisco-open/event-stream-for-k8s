use std::pin::pin;
use std::time::{Duration, UNIX_EPOCH};

use futures::future::{select, Either};
use futures::StreamExt;
use k8s_openapi::api::core::v1::Event;
use kube::runtime::watcher::{self, InitialListStrategy, ListSemantic};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client};
use once_cell::sync::Lazy;
use prometheus_exporter::prometheus::{
    opts, register_int_counter, register_int_counter_vec, register_int_gauge_vec, IntCounter,
    IntCounterVec, IntGaugeVec,
};
use sled::Batch;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::config::CONFIG;
use crate::types::{KesError, KubernetesEvent};
use crate::{u64_to_u8_arr, u8_slice_to_u64};

static PROM_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!("kube_event_stream_events_processed", "Events seen"),
        &["type"]
    )
    .unwrap()
});

static PROM_DB_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "kube_event_stream_cachedb_sync_bytes",
        "Bytes synced to cache"
    )
    .unwrap()
});

static PROM_DB_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        opts!(
            "kube_event_stream_cachedb_size",
            "On disk cache sizes, item count and total bytes."
        ),
        &["type"]
    )
    .unwrap()
});

pub(crate) async fn write_events(
    db: sled::Db,
    mut events_rx: Receiver<Event>,
    mut term_rx: broadcast::Receiver<()>,
) -> Result<(), KesError> {
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
            let event = KubernetesEvent::from(event);
            if db.contains_key(event.key().as_str())? {
                cache_hits += 1;
                continue;
            }
            cache_misses += 1;

            println!("{}", serde_json::to_string(&event)?);

            batch.insert(
                event.key().as_str(),
                &u64_to_u8_arr(UNIX_EPOCH.elapsed()?.as_secs()),
            );
        }
        db.apply_batch(batch)?;
        let bytes_synced = db.flush_async().await?;

        PROM_EVENTS
            .with_label_values(&["total"])
            .inc_by(events_count.try_into().unwrap());
        PROM_EVENTS
            .with_label_values(&["cache_hits"])
            .inc_by(cache_hits);
        PROM_EVENTS
            .with_label_values(&["cache_misses"])
            .inc_by(cache_misses);
        PROM_DB_BYTES.inc_by(bytes_synced.try_into().unwrap());

        eprintln!(
            "Processed {} events, out of which {} were already seen and {} were new. {} bytes synced to DB.",
            events_count, cache_hits, cache_misses, bytes_synced
        );
    }
}

pub(crate) async fn watch_events(
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

pub(crate) async fn clean_cache(
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

        PROM_DB_SIZE
            .with_label_values(&["items"])
            .set(db.len() as i64);

        PROM_DB_SIZE
            .with_label_values(&["bytes"])
            .set(db.size_on_disk()? as i64);

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
