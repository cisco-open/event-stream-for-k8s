# Copyright 2024 Cisco Systems, Inc. and its affiliates

from time import sleep
import pytest
import json

from integ_test_cluster import cluster


def build_event(pod):
    return {
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": "custom-event",
            "namespace": "default",
        },
        "involvedObject": {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": pod,
            "namespace": "default",
        },
        "reason": "CustomReason",
        "message": "This is a custom event",
        "source": {"component": "custom-component"},
        "type": "Normal",
    }


def parse_custom_events(cluster):
    logs: str = cluster.kubectl(
        ["logs", "kubernetes-event-stream-0", "-n", "test"], as_dict=False
    )

    events = []
    for line in logs.splitlines():
        event = json.loads(line)
        if "kubernetes_event" not in event:
            continue
        if event["kubernetes_event"]["metadata"]["name"] == "custom-event":
            events.append(event["kubernetes_event"])

    return events


def test_running(cluster):
    # Wait for the pod to start
    cluster.wait(
        "pod/kubernetes-event-stream-0",
        "condition=Ready=True",
        timeout=180,
        namespace="test",
    )

    # Emit 10 custom events
    for i in range(0, 10):
        cluster.apply(build_event(f"pod-{i}"))

    # Allow some time for events to be seen and processed
    sleep(5)

    # Fetch all custom events in the current pod logs
    events = parse_custom_events(cluster)

    # Ensure we got the same number of events we emitted
    assert len(events) == 10

    # Ensure their sequence numbers are correct
    assert sorted([event["involvedObject"]["name"] for event in events]) == sorted(
        [f"pod-{i}" for i in range(0, 10)]
    )

    # Delete the pod
    cluster.kubectl(
        ["delete", "pod", "kubernetes-event-stream-0", "-n", "test"], as_dict=False
    )

    # Wait for the pod to start again
    cluster.wait(
        "pod/kubernetes-event-stream-0",
        "condition=Ready=True",
        timeout=180,
        namespace="test",
    )

    sleep(5)

    # Parse the log, ensure no custom events exist (these should be deduped in the DB)
    assert len(parse_custom_events(cluster)) == 0

    # Emit 10 more events with bigger sequence numbers
    for i in range(10, 20):
        cluster.apply(build_event(f"pod-{i}"))

    sleep(5)

    # Parse and ensure we have what we expect in the logs
    events = parse_custom_events(cluster)

    assert len(events) == 10

    assert sorted([event["involvedObject"]["name"] for event in events]) == sorted(
        [f"pod-{i}" for i in range(10, 20)]
    )
