# kubernetes-event-stream

Kubernetes Event Stream reads Events from the Kubernetes API and logs them to standard output.

Perfect for cluster operators who want to log [Events](https://pkg.go.dev/k8s.io/api/events/v1#Event) to long-term storage, e.g., an ELK stack.

## Usage

`kubernetes-event-stream` is best deployed as a single-replica `StatefulSet` with persistent storage. For example:

```yaml
kind: StatefulSet
metadata:
  name: kubernetes-event-stream
spec:
  replicas: 1
  volumeClaimTemplates:
  - metadata:
      name: events-db
    spec:
      accessModes: [ReadWriteOnce]
      resources: {requests: {storage: 1Gi}}
      storageClassName: gp2  # or similar
  spec:
    containers:
    - name: main
      volumeMounts:
      - name: events-db
        mountPath: /events-db
```

### Environment variables

- `CACHE_TTL` (default `3600`): How long to cache Events.
- `CACHE_DB` (default `./events-db`): Path to the cache.
