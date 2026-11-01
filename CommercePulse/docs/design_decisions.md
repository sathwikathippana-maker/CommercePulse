# Design Decisions

1. **Event history before current state:** Bronze retains accepted source events; Silver is derived state.
2. **Version-based CDC:** highest `event_version` per transaction determines current state, rather than arrival order.
3. **Reversal as a state:** retain the event for auditability but exclude it from active metrics.
4. **Recompute impacted dates:** use old and new Silver dates for changed transaction IDs; recompute from current state to avoid double counting.
5. **Idempotency:** anti-join incoming event IDs against Bronze before append. A rerun of the same event does not change state or metrics.
6. **Quarantine:** malformed business records and event/version conflicts are kept out of Silver and stored with a rejection reason.
7. **Synthetic data:** deterministic generator supports reproducible 100K+ performance runs. It is not presented as a real external feed.
8. **Production caveat:** concurrent writers, schema registry, governed reference data, cloud credentials, alerting and orchestration are outside this local challenge implementation.
