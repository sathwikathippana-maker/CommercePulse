from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType
)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("event_version", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(12, 2), True),
])
