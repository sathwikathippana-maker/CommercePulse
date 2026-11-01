from pyspark.sql import functions as F

REQUIRED = [
    "event_id", "transaction_id", "event_version", "event_type", "event_ts",
    "transaction_date", "product_id", "customer_id", "quantity", "unit_price"
]

def validate_dataframe(df):
    """Split rows into accepted and rejected data with a reason column."""
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Input schema missing required columns: {missing}")

    checks = [
        (F.col("event_id").isNull() | (F.trim("event_id") == ""), "MISSING_EVENT_ID"),
        (F.col("transaction_id").isNull() | (F.trim("transaction_id") == ""), "MISSING_TRANSACTION_ID"),
        (F.col("event_version").isNull() | (F.col("event_version") < 1), "INVALID_EVENT_VERSION"),
        (~F.upper("event_type").isin("INSERT", "UPDATE", "REVERSAL"), "INVALID_EVENT_TYPE"),
        (F.to_timestamp("event_ts").isNull(), "INVALID_EVENT_TIMESTAMP"),
        (F.to_date("transaction_date").isNull(), "INVALID_TRANSACTION_DATE"),
        (F.col("product_id").isNull() | (F.trim("product_id") == ""), "MISSING_PRODUCT_ID"),
        (F.col("customer_id").isNull() | (F.trim("customer_id") == ""), "MISSING_CUSTOMER_ID"),
        (F.col("quantity").isNull() | (F.col("quantity") < 0), "INVALID_QUANTITY"),
        (F.col("unit_price").isNull() | (F.col("unit_price") < 0), "INVALID_UNIT_PRICE"),
    ]
    reason = F.lit(None).cast("string")
    for condition, label in checks:
        reason = F.when(reason.isNull() & condition, F.lit(label)).otherwise(reason)
    tagged = df.withColumn("rejection_reason", reason)
    return (
        tagged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason"),
        tagged.filter(F.col("rejection_reason").isNotNull())
    )
