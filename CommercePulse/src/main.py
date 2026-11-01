import argparse, os
from .spark_session import create_spark
from .pipeline import run_pipeline

def main():
    parser = argparse.ArgumentParser(description="Run CommercePulse incremental analytics")
    parser.add_argument("--input", required=True, help="Input CSV file or directory")
    parser.add_argument("--warehouse", default=os.getenv("COMMERCEPULSE_WAREHOUSE", "warehouse"))
    args = parser.parse_args()
    spark = create_spark()
    try:
        result = run_pipeline(spark, args.input, args.warehouse)
        print("\n=== CommercePulse run summary ===")
        for key, value in result.items():
            print(f"{key}: {value}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
