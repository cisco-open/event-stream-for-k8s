use k8s_openapi::api::core::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::serde::Serialize;
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, Serialize)]
pub(crate) struct KubernetesEvent {
    pub(crate) time: Time,
    pub(crate) kubernetes_event: Event,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum KesError {
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

/// Timestamp events [like Fluent Bit does](https://docs.fluentbit.io/manual/pipeline/inputs/kubernetes-events#event-timestamp):
/// > Event timestamp will be created from the first existing field
/// > in the following order of precendence:
/// > lastTimestamp, firstTimestamp, metadata.creationTimestamp
pub(crate) fn timestamp(ev: &Event) -> Time {
    [
        &ev.last_timestamp,
        &ev.first_timestamp,
        &ev.metadata.creation_timestamp,
    ]
    .into_iter()
    .find_map(|opt| opt.clone())
    // For now, assume _all_ Events have at least one usable timestamp.
    // An Event that doesn't would act as a poison pill and repeatedly
    // crash the program, but the possibility is incredibly remote.
    .expect("Somehow, this Event has all three time fields unset")
}
