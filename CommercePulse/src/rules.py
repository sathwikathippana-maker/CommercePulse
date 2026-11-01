"""Pure Python business rules, intentionally independent of Spark."""
from datetime import date
from decimal import Decimal, InvalidOperation

EVENT_TYPES = {"INSERT", "UPDATE", "REVERSAL"}

def validate_event(row):
    """Return a list of validation errors for a normalized event dictionary."""
    errors = []
    required = ("event_id", "transaction_id", "event_version", "event_type",
                "event_ts", "transaction_date", "product_id", "customer_id",
                "quantity", "unit_price")
    for field in required:
        if row.get(field) is None or str(row.get(field)).strip() == "":
            errors.append(f"MISSING_{field.upper()}")
    if errors:
        return errors
    if str(row["event_type"]).upper() not in EVENT_TYPES:
        errors.append("INVALID_EVENT_TYPE")
    try:
        if int(row["event_version"]) < 1:
            errors.append("INVALID_EVENT_VERSION")
    except (TypeError, ValueError):
        errors.append("INVALID_EVENT_VERSION")
    try:
        if int(row["quantity"]) < 0:
            errors.append("NEGATIVE_QUANTITY")
    except (TypeError, ValueError):
        errors.append("INVALID_QUANTITY")
    try:
        price = Decimal(str(row["unit_price"]))
        if not price.is_finite() or price < 0:
            errors.append("INVALID_UNIT_PRICE")
    except (InvalidOperation, TypeError, ValueError):
        errors.append("INVALID_UNIT_PRICE")
    try:
        date.fromisoformat(str(row["transaction_date"]))
    except ValueError:
        errors.append("INVALID_TRANSACTION_DATE")
    if str(row["event_type"]).upper() == "REVERSAL":
        # Reversal rows retain the original transaction identifiers; amount is ignored in metrics.
        pass
    return errors

def choose_latest(events):
    """Select highest event_version for a transaction; reject tied conflicting payloads."""
    if not events:
        return None
    ordered = sorted(events, key=lambda x: int(x["event_version"]))
    highest = int(ordered[-1]["event_version"])
    winners = [x for x in ordered if int(x["event_version"]) == highest]
    signatures = {
        tuple(str(x.get(k, "")) for k in (
            "event_id", "event_type", "transaction_date", "product_id",
            "customer_id", "quantity", "unit_price"
        ))
        for x in winners
    }
    if len(signatures) > 1:
        raise ValueError("CONFLICTING_TRANSACTION_VERSION")
    return winners[-1]

def active_metrics(events):
    """Return metric aggregates for non-reversed transaction dictionaries."""
    active = [e for e in events if str(e["event_type"]).upper() != "REVERSAL"]
    revenue = sum(Decimal(str(e["quantity"])) * Decimal(str(e["unit_price"])) for e in active)
    return {
        "transaction_count": len(active),
        "units_sold": sum(int(e["quantity"]) for e in active),
        "gross_revenue": str(revenue.quantize(Decimal("0.01"))),
        "active_customer_count": len({e["customer_id"] for e in active}),
    }
