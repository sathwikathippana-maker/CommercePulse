import argparse, csv, random, os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

FIELDS = ["event_id","transaction_id","event_version","event_type","event_ts",
          "transaction_date","product_id","customer_id","quantity","unit_price"]

def generate_rows(rows, seed=42):
    """Yield rows events; periodic version-2 events target an existing transaction."""
    random.seed(seed)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(rows):
        is_correction = i > 0 and i % 20 == 0
        is_reversal = is_correction and i % 100 == 0
        target_index = i - 1 if is_correction else i
        tx_date = (start + timedelta(days=(target_index % 30))).date().isoformat()
        ts = start + timedelta(minutes=i)
        yield {
            "event_id": f"EV{i:010d}",
            "transaction_id": f"TX{target_index:08d}",
            "event_version": 2 if is_correction else 1,
            "event_type": "REVERSAL" if is_reversal else ("UPDATE" if is_correction else "INSERT"),
            "event_ts": ts.isoformat(),
            "transaction_date": tx_date,
            "product_id": f"PROD{random.randint(1, 100):03d}",
            "customer_id": f"CUST{random.randint(1, 10000):05d}",
            "quantity": random.randint(1, 5),
            "unit_price": str(Decimal(random.randint(100, 50000)) / Decimal(100)),
        }

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=100000)
    p.add_argument("--output", default="data/generated/batch_100k.csv")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    if args.rows < 1:
        p.error("--rows must be at least 1")
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(generate_rows(args.rows, args.seed))
    print(f"Wrote {args.rows} events to {args.output}")

if __name__ == "__main__":
    main()
