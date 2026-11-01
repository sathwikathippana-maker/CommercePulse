"""Emit synthetic event micro-batches to demonstrate periodic arrivals."""
import argparse, csv, os, random, time
from datetime import datetime, timezone
from decimal import Decimal

FIELDS = ["event_id","transaction_id","event_version","event_type","event_ts",
          "transaction_date","product_id","customer_id","quantity","unit_price"]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="data/live_batches")
    p.add_argument("--interval", type=float, default=3)
    p.add_argument("--batches", type=int, default=5)
    p.add_argument("--events-per-batch", type=int, default=10)
    args = p.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    for b in range(args.batches):
        path = os.path.join(args.output_dir, f"batch_{b+1:04d}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS); w.writeheader()
            for j in range(args.events_per_batch):
                n = b * args.events_per_batch + j
                now = datetime.now(timezone.utc)
                w.writerow({
                    "event_id": f"SIM-EV-{n:08d}", "transaction_id": f"SIM-TX-{n:08d}",
                    "event_version": 1, "event_type": "INSERT", "event_ts": now.isoformat(),
                    "transaction_date": now.date().isoformat(), "product_id": f"PROD{random.randint(1,20):03d}",
                    "customer_id": f"CUST{random.randint(1,500):05d}", "quantity": random.randint(1,4),
                    "unit_price": str(Decimal(random.randint(500,20000))/Decimal(100))
                })
        print(f"Created {path}")
        if b < args.batches - 1: time.sleep(args.interval)

if __name__ == "__main__":
    main()
