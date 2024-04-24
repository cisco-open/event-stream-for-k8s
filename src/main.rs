use std::pin::pin;

use futures::TryStreamExt;
use k8s_openapi::api::core::v1::Event;
use kube::runtime::watcher::{self, InitialListStrategy, ListSemantic};
use kube::runtime::WatchStreamExt;
use kube::Api;

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

    let db = sled::open("k8s-events.db")?;

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

    while let Some(event) = stream.try_next().await? {
        let json_event = serde_json::to_string(&event)?;
        let event_hash = u64_to_u8_arr(hasher.hash_one(&json_event));
        if db.contains_key(event_hash)? {
            eprint!(".");
            continue;
        }
        eprint!("+");
        println!("{}", json_event);
        db.insert(event_hash, "")?;
    }
    Ok(())
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
