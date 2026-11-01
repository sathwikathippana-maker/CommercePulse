from .spark_session import create_spark
def main():
    spark = create_spark("CommercePulseSmokeTest")
    try:
        rows = [(1, "commercepulse")]
        df = spark.createDataFrame(rows, ["id", "name"])
        assert df.count() == 1
        print("Spark smoke test passed:", df.collect())
    finally:
        spark.stop()
if __name__ == "__main__":
    main()
