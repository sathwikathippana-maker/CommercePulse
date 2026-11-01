import pytest
from src.rules import validate_event, choose_latest, active_metrics

def valid(**overrides):
    row = {
        "event_id": "E1", "transaction_id": "T1", "event_version": 1,
        "event_type": "INSERT", "event_ts": "2026-01-01T10:00:00Z",
        "transaction_date": "2026-01-01", "product_id": "P1",
        "customer_id": "C1", "quantity": 2, "unit_price": "10.00"
    }
    row.update(overrides)
    return row

def test_valid_event_passes():
    assert validate_event(valid()) == []

def test_missing_customer_is_rejected():
    assert "MISSING_CUSTOMER_ID" in validate_event(valid(customer_id=""))

def test_negative_quantity_is_rejected():
    assert "NEGATIVE_QUANTITY" in validate_event(valid(quantity=-1))

def test_latest_version_wins():
    old = valid()
    new = valid(event_id="E2", event_version=2, event_type="UPDATE", quantity=5)
    assert choose_latest([old, new])["event_id"] == "E2"

def test_same_version_conflict_raises():
    a = valid()
    b = valid(event_id="E2", quantity=3)
    with pytest.raises(ValueError, match="CONFLICTING_TRANSACTION_VERSION"):
        choose_latest([a, b])

def test_reversal_excluded_from_metrics():
    rows = [valid(), valid(event_id="E2", event_version=2, event_type="REVERSAL")]
    metrics = active_metrics(rows)
    assert metrics["transaction_count"] == 1
    assert metrics["units_sold"] == 2
    assert metrics["gross_revenue"] == "20.00"

def test_late_update_can_move_business_date():
    old = valid()
    late = valid(event_id="E2", event_version=2, event_type="UPDATE",
                 transaction_date="2026-01-03")
    assert old["transaction_date"] != choose_latest([old, late])["transaction_date"]
