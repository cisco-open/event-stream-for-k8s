// Copyright 2024 Cisco Systems, Inc. and its affiliates

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use k8s_openapi::api::core::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWrite};

#[allow(dead_code)]
#[path = "../src/config.rs"]
mod config;
#[allow(dead_code)]
#[path = "../src/tasks.rs"]
mod tasks;
#[allow(dead_code)]
#[path = "../src/types.rs"]
mod types;

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

    tasks::write_events_to(db, events_rx, term_tx.subscribe(), events_out)
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

    let err = tasks::write_events_to(db, events_rx, term_tx.subscribe(), BrokenWriter)
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

#[allow(dead_code)]
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
