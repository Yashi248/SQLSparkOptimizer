"""
Phase 0 — Spark smoke test.

Confirms three things on this machine:
  1. PySpark can start a local SparkSession (Java is wired up correctly).
  2. The Spark UI is reachable at http://localhost:4040 (it pauses so you can look).
  3. Spark can read the TPC-H Parquet that tpch_setup.py wrote.

Run AFTER `python data/tpch_setup.py`:
    python data/spark_smoke.py
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

PARQUET_DIR = Path(__file__).resolve().parent / "tpch"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("phase0-smoke")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")  
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"\nSpark {spark.version} is up. UI: http://localhost:4040\n")

    lineitem = PARQUET_DIR / "lineitem.parquet"
    if lineitem.exists():
        df = spark.read.parquet(lineitem.as_posix())
        print(f"lineitem rows: {df.count():,}")
        df.show(5)
    else:
        print(f"(No TPC-H data at {PARQUET_DIR}. Run data/tpch_setup.py first.)")

    input("\nSpark UI is live at http://localhost:4040  press Enter to shut down...")
    spark.stop()


if __name__ == "__main__":
    main()
