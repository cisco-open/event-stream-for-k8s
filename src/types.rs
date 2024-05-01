use k8s_openapi::api::core::v1::Event;
use k8s_openapi::serde::Serialize;
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, Serialize)]
pub(crate) struct KubernetesEvent {
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
