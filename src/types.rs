use k8s_openapi::api::core::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::chrono::Utc;
use k8s_openapi::serde::Serialize;
use tokio::sync::mpsc::error::SendError;
use tracing::warn;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KubernetesEvent {
    pub(crate) time: Time,
    pub(crate) kubernetes_event: Event,
}

impl KubernetesEvent {
    pub(crate) fn key(&self) -> String {
        format!(
            "{}:{}",
            self.kubernetes_event
                .metadata
                .uid
                .as_ref()
                .unwrap_or(&String::default()),
            self.kubernetes_event
                .metadata
                .resource_version
                .as_ref()
                .unwrap_or(&String::default())
        )
    }
}

impl From<Event> for KubernetesEvent {
    fn from(ev: Event) -> Self {
        // Timestamp events like Fluent Bit does:
        // https://docs.fluentbit.io/manual/pipeline/inputs/kubernetes-events#event-timestamp
        let time = ev.last_timestamp.as_ref().cloned().unwrap_or(
            ev.first_timestamp.as_ref().cloned().unwrap_or(
                ev.metadata
                    .creation_timestamp
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| {
                        warn!("No timestamp in event? Using current time.");
                        Time(Utc::now())
                    }),
            ),
        );

        Self {
            time,
            kubernetes_event: ev,
        }
    }
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
