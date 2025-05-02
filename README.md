# event-stream-for-k8s

Event Stream for K8s reads Events from the Kubernetes API and logs them to standard output.

Perfect for cluster operators who want to log [Events](https://pkg.go.dev/k8s.io/api/events/v1#Event) to long-term storage, e.g., an ELK stack.

## Usage

`event-stream-for-k8s` is best deployed as a single-replica `StatefulSet` with persistent storage. For example:

```yaml
kind: StatefulSet
metadata:
  name: event-stream-for-k8s
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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Distributed under the Apache 2.0 license. See [LICENSE](LICENSE) for more information.
