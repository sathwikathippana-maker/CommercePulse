# Test Plan

| Scenario | Coverage |
|---|---|
| Valid event | `test_valid_event_passes` |
| Missing required value | `test_missing_customer_is_rejected` |
| Negative quantity | `test_negative_quantity_is_rejected` |
| Higher version | `test_latest_version_wins` |
| Same-version conflict | `test_same_version_conflict_raises` |
| Reversal | `test_reversal_excluded_from_metrics` |
| Late update/date movement | `test_late_update_can_move_business_date` |

Additional integration scenarios to execute before submission: empty CSV, malformed timestamp, duplicate event delivery, invalid schema, partial failure/retry, rerun, and 100K+ generator. Record actual results in the submission note; do not claim unrun tests passed.
