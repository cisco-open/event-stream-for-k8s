// Copyright 2024 Cisco Systems, Inc. and its affiliates

use std::pin::pin;
use std::time::{Duration, UNIX_EPOCH};

use futures::StreamExt;
use futures::future::{Either, select};
use k8s_openapi::api::core::v1::Event;
use kube::runtime::WatchStreamExt;
use kube::runtime::watcher::{self, InitialListStrategy, ListSemantic};
use kube::{Api, Client};
use once_cell::sync::Lazy;
use prometheus_exporter::prometheus::{
    IntCounter, IntCounterVec, IntGaugeVec, opts, register_int_counter, register_int_counter_vec,
    register_int_gauge_vec,
};
use sled::Batch;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};

use crate::config::CONFIG;
use crate::types::{KesError, KubernetesEvent};
use crate::{u8_slice_to_u64, u64_to_u8_arr};

static PROM_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!("kube_event_stream_events_count", "Events types seen."),
        &[
            "event_type",
            "event_reason",
            "event_kind",
            "event_namespace"
        ]
    )
    .unwrap()
});

static PROM_DB_PROC: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!("kube_event_stream_cachedb_events_processed", "Events seen"),
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
    events_rx: Receiver<Event>,
    term_rx: broadcast::Receiver<()>,
) -> Result<(), KesError> {
    write_events_to(db, events_rx, term_rx, tokio::io::stdout()).await
}

pub(crate) async fn write_events_to<W>(
    db: sled::Db,
    mut events_rx: Receiver<Event>,
    mut term_rx: broadcast::Receiver<()>,
    mut events_out: W,
) -> Result<(), KesError>
where
    W: AsyncWrite + Unpin,
{
    info!("Starting event writer task...");
    loop {
        let mut events = vec![];

        let events_count = match select(
            pin!(events_rx.recv_many(&mut events, 1024)),
            pin!(term_rx.recv()),
        )
        .await
        {
            Either::Left((0, _)) => {
                warn!("Event channel dropped, stopping event writer task!");
                return Ok(());
            }
            Either::Left((c, _)) => c,
            Either::Right(_) => {
                info!("Stopping event writer task!");
                return Ok(());
            }
        };

        let mut batch = Batch::default();
        let mut cache_hits: u64 = 0;
        let mut cache_misses: u64 = 0;
        let mut event_output = Vec::new();
        for event in events {
            let event = KubernetesEvent::from(event);
            let key = event.key();
            if db.contains_key(key.as_str())? {
                cache_hits += 1;
                continue;
            }
            cache_misses += 1;

            serde_json::to_writer(&mut event_output, &event)?;
            event_output.push(b'\n');

            PROM_EVENTS
                .with_label_values(&[
                    &event.kubernetes_event.type_.unwrap_or("-".to_string()),
                    &event.kubernetes_event.reason.unwrap_or("-".to_string()),
                    &event
                        .kubernetes_event
                        .involved_object
                        .kind
                        .unwrap_or("-".to_string()),
                    &event
                        .kubernetes_event
                        .involved_object
                        .namespace
                        .unwrap_or("-".to_string()),
                ])
                .inc();

            batch.insert(
                key.as_str(),
                &u64_to_u8_arr(UNIX_EPOCH.elapsed()?.as_secs()),
            );
        }

        if !event_output.is_empty() {
            events_out.write_all(&event_output).await?;
            events_out.flush().await?;
        }

        db.apply_batch(batch)?;
        let bytes_synced = db.flush_async().await?;

        PROM_DB_PROC
            .with_label_values(&["total"])
            .inc_by(events_count.try_into().unwrap());
        PROM_DB_PROC
            .with_label_values(&["cache_hits"])
            .inc_by(cache_hits);
        PROM_DB_PROC
            .with_label_values(&["cache_misses"])
            .inc_by(cache_misses);
        PROM_DB_BYTES.inc_by(bytes_synced.try_into().unwrap());

        debug!(
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
    info!("Starting event watcher task...");

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
            Either::Left((Some(Ok(event)), _)) => events_tx.send(event).await.map_err(Box::new)?,
            Either::Left((Some(Err(e)), _)) => {
                warn!("Error receiving events: {:?}", e);
            }
            Either::Left((None, _)) | Either::Right(_) => {
                warn!("Stopping event watcher task!");
                return Ok(());
            }
        }
    }
}

pub(crate) async fn clean_cache(
    db: sled::Db,
    mut term_rx: broadcast::Receiver<()>,
) -> Result<(), KesError> {
    info!("Starting cache cleaner task...");
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
            debug!(
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
            info!("Stopping cache cleaner task!");
            return Ok(());
        };
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::{Context, Poll};
    use std::time::{SystemTime, UNIX_EPOCH};

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWrite};

    use super::*;

    static TEMP_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn write_events_to_emits_events_as_stdout_ndjson() {
        let db_path = temp_db_path();
        let db = sled::open(&db_path).expect("test db should open");
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(1);
        let (term_tx, _) = tokio::sync::broadcast::channel(1);
        let (events_out, mut events_in) = tokio::io::duplex(4096);

        events_tx
            .send(test_event("event-uid", "42"))
            .await
            .expect("event should send");
        drop(events_tx);

        write_events_to(db, events_rx, term_tx.subscribe(), events_out)
            .await
            .expect("event writer should succeed");
        drop(term_tx);

        let mut output = String::new();
        events_in
            .read_to_string(&mut output)
            .await
            .expect("event output should be readable");

        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1, "expected exactly one event line: {output}");
        assert!(
            output.ends_with('\n'),
            "event output should be newline-delimited: {output}"
        );

        let event: Value = serde_json::from_str(lines[0]).expect("event line should be JSON");
        assert_eq!(event["kubernetes_event"]["metadata"]["uid"], "event-uid");
        assert_eq!(
            event["kubernetes_event"]["metadata"]["resourceVersion"],
            "42"
        );

        let _ = std::fs::remove_dir_all(db_path);
    }

    #[tokio::test]
    async fn write_events_to_propagates_output_errors() {
        let db_path = temp_db_path();
        let db = sled::open(&db_path).expect("test db should open");
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(1);
        let (term_tx, _) = tokio::sync::broadcast::channel(1);

        events_tx
            .send(test_event("event-uid", "42"))
            .await
            .expect("event should send");
        drop(events_tx);

        let err = write_events_to(db, events_rx, term_tx.subscribe(), BrokenWriter)
            .await
            .expect_err("event output errors should propagate");

        assert!(err.to_string().contains("stdout failed"));

        let _ = std::fs::remove_dir_all(db_path);
    }

    fn test_event(uid: &str, resource_version: &str) -> Event {
        Event {
            metadata: ObjectMeta {
                uid: Some(uid.to_string()),
                resource_version: Some(resource_version.to_string()),
                ..Default::default()
            },
            reason: Some("TestReason".to_string()),
            type_: Some("Normal".to_string()),
            ..Default::default()
        }
    }

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "event-stream-for-k8s-db-test-{}-{}-{}",
            std::process::id(),
            TEMP_DB_COUNTER.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after Unix epoch")
                .as_nanos()
        ))
    }

    struct BrokenWriter;

    impl AsyncWrite for BrokenWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "stdout failed",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
}
