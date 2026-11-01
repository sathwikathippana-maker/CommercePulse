import os, uuid, time
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from .schemas import EVENT_SCHEMA
from .validation import validate_dataframe

def _exists(spark, path):
    return spark._jsparkSession.sessionState().catalog().tableExists(path) if False else os.path.exists(path)

def _read_input(spark, input_path):
    return (spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(EVENT_SCHEMA)
            .csv(input_path)
            .withColumn("source_file", F.input_file_name())
            .withColumn("ingested_at", F.current_timestamp()))

def _read_delta_or_empty(spark, path, schema):
    if os.path.exists(path):
        return spark.read.format("delta").load(path)
    return spark.createDataFrame([], schema)

def _write_delta(df, path, mode="append"):
    (df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path))

def run_pipeline(spark, input_path, warehouse="warehouse"):
    started = time.time()
    run_id = str(uuid.uuid4())
    paths = {
        "bronze": f"{warehouse}/bronze/events",
        "silver": f"{warehouse}/silver/transactions",
        "product": f"{warehouse}/gold/daily_product_metrics",
        "customer": f"{warehouse}/gold/daily_customer_metrics",
        "quarantine": f"{warehouse}/quarantine/events",
        "runlog": f"{warehouse}/ops/run_log",
    }
    incoming = _read_input(spark, input_path)
    accepted, rejected = validate_dataframe(incoming)
    accepted = accepted.withColumn("event_type", F.upper("event_type"))
    # Detect event_id conflicts within this batch before deduplicating.
    payload_cols = ["transaction_id", "event_version", "event_type", "event_ts",
                    "transaction_date", "product_id", "customer_id", "quantity", "unit_price"]
    event_conflicts = (accepted.groupBy("event_id")
                       .agg(F.countDistinct(F.sha2(F.to_json(F.struct(*payload_cols)), 256)).alias("variants"))
                       .filter("variants > 1").select("event_id"))
    conflict_rows = accepted.join(event_conflicts, "event_id", "inner").withColumn(
        "rejection_reason", F.lit("CONFLICTING_EVENT_ID"))
    rejected = rejected.unionByName(conflict_rows, allowMissingColumns=True)
    accepted = accepted.join(event_conflicts, "event_id", "left_anti")
    # Keep one copy of exact duplicate event rows in the batch.
    accepted = accepted.dropDuplicates(["event_id"] + payload_cols)

    old_silver = _read_delta_or_empty(spark, paths["silver"], EVENT_SCHEMA)
    # Identify previously ingested event IDs; repeated deliveries are idempotently ignored.
    bronze_old = _read_delta_or_empty(spark, paths["bronze"], EVENT_SCHEMA)
    if os.path.exists(paths["bronze"]):
        accepted_new = accepted.join(bronze_old.select("event_id").distinct(), "event_id", "left_anti")
    else:
        accepted_new = accepted

    # Same transaction/version with multiple distinct events is a conflict.
    version_conflicts = (accepted_new.groupBy("transaction_id", "event_version")
                         .agg(F.countDistinct("event_id").alias("n"))
                         .filter("n > 1").select("transaction_id", "event_version"))
    version_bad = accepted_new.join(version_conflicts, ["transaction_id", "event_version"], "inner") \
        .withColumn("rejection_reason", F.lit("CONFLICTING_TRANSACTION_VERSION"))
    rejected = rejected.unionByName(version_bad, allowMissingColumns=True)
    accepted_new = accepted_new.join(version_conflicts, ["transaction_id", "event_version"], "left_anti")

    # Append only unseen accepted events to Bronze.
    if accepted_new.limit(1).count():
        _write_delta(accepted_new, paths["bronze"], "append")
    if rejected.limit(1).count():
        _write_delta(rejected, paths["quarantine"], "append")

    # Current state is highest event_version across complete Bronze history.
    all_bronze = _read_delta_or_empty(spark, paths["bronze"], EVENT_SCHEMA)
    if all_bronze.limit(1).count():
        w = Window.partitionBy("transaction_id").orderBy(F.col("event_version").desc(), F.col("event_ts").desc(), F.col("event_id").desc())
        new_silver = (all_bronze.withColumn("rn", F.row_number().over(w))
                      .filter("rn = 1").drop("rn"))
    else:
        new_silver = all_bronze

    # Determine affected dates from old and new states for changed transaction IDs.
    changed_ids = accepted_new.select("transaction_id").distinct()
    if changed_ids.limit(1).count():
        old_dates = old_silver.join(changed_ids, "transaction_id", "inner").select("transaction_date")
        new_dates = new_silver.join(changed_ids, "transaction_id", "inner").select("transaction_date")
        affected_dates = old_dates.union(new_dates).distinct().filter(F.col("transaction_date").isNotNull())
    else:
        affected_dates = spark.createDataFrame([], "transaction_date string")

    # Persist the latest state as a complete snapshot. Delta overwrite is deterministic and restartable.
    if new_silver.limit(1).count():
        _write_delta(new_silver, paths["silver"], "overwrite")

    # Rebuild metrics for affected dates from current Silver state, not additive incoming batch sums.
    if affected_dates.limit(1).count():
        active = new_silver.filter(F.upper("event_type") != "REVERSAL") \
            .withColumn("business_date", F.to_date("transaction_date")) \
            .join(affected_dates.withColumn("business_date", F.to_date("transaction_date")).select("business_date").distinct(),
                  "business_date", "inner") \
            .withColumn("gross_revenue", F.col("quantity") * F.col("unit_price"))

        product_metrics = (active.groupBy("business_date", "product_id")
            .agg(F.sum("gross_revenue").cast("decimal(18,2)").alias("gross_revenue"),
                 F.countDistinct("transaction_id").alias("transaction_count"),
                 F.sum("quantity").cast("long").alias("units_sold"),
                 F.countDistinct("customer_id").alias("active_customer_count")))
        customer_metrics = (active.groupBy("business_date", "customer_id")
            .agg(F.sum("gross_revenue").cast("decimal(18,2)").alias("gross_revenue"),
                 F.countDistinct("transaction_id").alias("transaction_count"),
                 F.sum("quantity").cast("long").alias("units_sold")))
        # Replace only impacted dates: preserve all other dates already in Gold.
        for key, metrics in (("product", product_metrics), ("customer", customer_metrics)):
            old_gold = _read_delta_or_empty(spark, paths[key], metrics.schema)
            remaining = old_gold.join(
                affected_dates.withColumn("business_date", F.to_date("transaction_date")).select("business_date").distinct(),
                "business_date", "left_anti")
            rebuilt = remaining.unionByName(metrics, allowMissingColumns=True)
            _write_delta(rebuilt, paths[key], "overwrite")

    accepted_count = accepted_new.count()
    rejected_count = rejected.count()
    duration = round(time.time() - started, 3)
    log_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("input_path", StringType(), True),
        StructField("started_at_utc", StringType(), True),
        StructField("accepted_new_count", LongType(), True),
        StructField("rejected_count", LongType(), True),
        StructField("duration_seconds", StringType(), True),
        StructField("status", StringType(), True),
    ])
    log = spark.createDataFrame([(
        run_id, input_path, datetime.now(timezone.utc).isoformat(),
        accepted_count, rejected_count, str(duration), "SUCCESS"
    )], log_schema)
    _write_delta(log, paths["runlog"], "append")
    return {"run_id": run_id, "accepted_new_count": accepted_count,
            "rejected_count": rejected_count, "duration_seconds": duration,
            "affected_dates": [r["transaction_date"] for r in affected_dates.collect()]}

