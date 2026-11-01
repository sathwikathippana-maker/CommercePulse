# CommercePulse — Incremental Commerce Analytics Platform

A local-first data engineering project for the YottaFlex AI code challenge. It demonstrates incremental transaction processing, event validation, duplicate handling, late-arriving updates, reversals, daily product/customer metrics, idempotent reruns, reconciliation, tests, and performance measurement.

## What it does

1. Reads transaction event CSV batches.
2. Preserves accepted raw events in a Bronze Delta table.
3. Validates schema and business rules; writes rejected rows to a quarantine CSV/Delta location.
4. Maintains a Silver current-state table using the highest valid `event_version` per transaction.
5. Recomputes only affected business dates in Gold after changes, including both the old and new date if a transaction is moved.
6. Produces daily product and customer metrics.
7. Records run-level operational metrics and supports safe reruns.

**Data source:** included reproducible synthetic commerce events. This is not a live retailer feed. The generator can create 100K+ event records, including version-2 updates and reversals that refer to earlier transaction IDs. The optional event simulator emits small batches periodically to demonstrate streaming-style arrival; it is a simulation, not a real external stream.

## Architecture

```text
CSV event batches / synthetic producer
              |
              v
      Bronze: raw event history
              |
              v
    Schema + business validation
         |             |
         |             +----> Quarantine
         v
    Silver: latest transaction state
              |
      impacted business dates
              |
              v
    Gold: daily product/customer metrics
              |
       reconciliation + run log
```

## Requirements

- Python 3.11 (recommended; Python 3.12 may work depending on Spark/JDK compatibility)
- Java 17
- Git
- Linux/macOS or GitHub Codespaces; Windows works with suitable Java/Python setup

Python 3.14 is not recommended for this pinned PySpark 3.5.3 environment.

## Setup in GitHub Codespaces (Bash)

Check Python versions:

```bash
python --version
ls /usr/bin/python*
```

If Python 3.11 is available:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If only Python 3.14 is available, use a Codespace image/devcontainer with Python 3.11, or install Python 3.11 before creating the environment. Do not reuse a virtual environment created with another Python version.

Verify:

```bash
python --version
java -version
python -c "import pyspark; print(pyspark.__version__)"
python -c "import delta; print('Delta Lake import OK')"
pytest -q
```

## Run the pipeline

Run the two sample batches in order. The output is written under `warehouse/`.

```bash
python -m src.main --input data/sample/batch_001.csv
python -m src.main --input data/sample/batch_002.csv
```

Run the same batch again to demonstrate idempotency:

```bash
python -m src.main --input data/sample/batch_002.csv
```

Inspect output:

```text
warehouse/
  bronze/events/
  silver/transactions/
  gold/daily_product_metrics/
  gold/daily_customer_metrics/
  quarantine/
  ops/run_log/
```

The pipeline creates the warehouse directory automatically. Delta tables are local directories; do not manually edit their contents.

## Generate larger data

```bash
python scripts/generate_data.py --rows 100000 --output data/generated/batch_100k.csv --seed 42
```

Then run:

```bash
python -m src.main --input data/generated/batch_100k.csv
```

## Optional simulated event producer

Generate a new CSV batch every few seconds:

```bash
python scripts/event_simulator.py --output-dir data/live_batches --interval 3 --batches 5 --events-per-batch 10
```

Process each batch manually:

```bash
for f in data/live_batches/*.csv; do
  python -m src.main --input "$f"
done
```

This demonstrates incremental micro-batches. It does not connect to a real payment platform or retailer.

## Tests

The unit tests for deterministic business rules do not require Spark:

```bash
pytest -q
```

The integration smoke test requires the pinned PySpark/Delta dependencies and Java 17:

```bash
python -m src.smoke_test
```

## Event contract

| Field | Type | Meaning |
|---|---|---|
| event_id | string | Unique event identifier |
| transaction_id | string | Stable transaction/business key |
| event_version | integer | Monotonically increasing version per transaction |
| event_type | string | INSERT, UPDATE, or REVERSAL |
| event_ts | timestamp string | When the source event occurred |
| transaction_date | YYYY-MM-DD | Business date used for reporting |
| product_id | string | Product key |
| customer_id | string | Customer key |
| quantity | integer | Units purchased |
| unit_price | decimal | Unit price in INR |
| source_file | string | Input file name, populated by ingestion |

### Business rules

- A transaction's highest valid `event_version` defines its current state.
- An exact repeated `event_id` is ignored after the first successful ingestion.
- The same `event_id` with different business payloads is quarantined as a conflict.
- A repeated transaction/version with different event IDs is quarantined as a version conflict.
- `REVERSAL` is a state, not a physical delete; reversed transactions do not contribute to active metrics.
- `UPDATE` may change product, customer, amount, or business date.
- If an update moves a transaction to another date, both old and new dates are recomputed.
- Metrics are rebuilt from Silver state for impacted dates, not incremented from incoming batch totals.

## Metrics

At date/product grain and date/customer grain:
- `gross_revenue`: sum(quantity × unit_price) for active transactions
- `transaction_count`: distinct active transactions
- `units_sold`: sum(quantity)
- `active_customer_count`: distinct customers (product metric only)

## Design limitations / production extensions

- Local Delta storage is used for reproducibility; cloud object storage can replace the warehouse root.
- Input is micro-batch CSV, not a production streaming broker.
- Reference data validation is limited to required identifiers and numeric/domain checks; production systems should join against governed product/customer master data.
- The run log is append-only, but concurrent writers require orchestration/locking in a production deployment.
- For a real deployment, add authentication, secrets management, schema registry, alerting, and a scheduler such as Airflow or Azure Data Factory.

## Suggested GitHub submission checklist

- [ ] Run all tests and the smoke test in a clean environment.
- [ ] Run sample batches in order and capture output/reconciliation.
- [ ] Run a 100K+ data test and record elapsed time and row counts.
- [ ] Review generated files; do not commit `.venv/` or the `warehouse/` directory.
- [ ] Add screenshots or a short terminal demo if useful.
- [ ] Explain the design decisions and limitations honestly.
