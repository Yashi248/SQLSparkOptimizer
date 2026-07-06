# Databricks notebook source
# MAGIC %md
# MAGIC # SQLSpark Optimizer on Databricks
# MAGIC Validates the tool on a real Spark cluster. It creates a big↔small join
# MAGIC (self-contained — no external data), forces the sort-merge anti-pattern,
# MAGIC and runs `optimize()` to detect → apply the broadcast fix → prove the output
# MAGIC is identical → measure the speedup.
# MAGIC
# MAGIC Works on **Databricks Community Edition** (free).

# COMMAND ----------
# MAGIC %md ### 1. Install the package
# MAGIC `--no-deps` so we don't touch the cluster's built-in PySpark/MLflow; then the
# MAGIC two libraries the cluster doesn't already have.

# COMMAND ----------
# MAGIC %pip install --no-deps git+https://github.com/Yashi248/SQLSparkOptimizer.git
# MAGIC %pip install sqlglot langgraph

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ### 2. Build a big↔small join and expose the anti-pattern
# MAGIC `autoBroadcastJoinThreshold=-1` forces Spark to sort-merge the join (the
# MAGIC missed-broadcast case), exactly what the optimizer should catch.

# COMMAND ----------
from pyspark.sql import functions as F

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", "false")  # static, readable plan

spark.range(0, 20_000_000).withColumn("k", (F.col("id") % 500)) \
    .createOrReplaceTempView("events")            # ~large fact
spark.range(0, 500).withColumnRenamed("id", "k") \
    .withColumn("name", F.concat(F.lit("k"), F.col("k").cast("string"))) \
    .createOrReplaceTempView("dim")               # ~tiny dimension

# COMMAND ----------
# MAGIC %md ### 3. Optimize — detect, fix, validate, measure
# MAGIC No `parquet_dir` needed: table sizing comes from Spark's own stats.

# COMMAND ----------
from sqlspark_optimizer import optimize

sql = ("SELECT d.name, COUNT(*) AS c "
       "FROM events e JOIN dim d ON e.k = d.k "
       "GROUP BY d.name")

result = optimize(sql, spark, source_dialect="spark",
                  timing_runs=1, use_llm_explain=False)

print("applied rules :", result.applied_rules)
print("speedup       :", f"{result.speedup:.2f}x")
print("validated     :", result.status != "reverted")
print("explanation   :", result.explanation)
print("\noptimized SQL:\n", result.optimized_sql)

# COMMAND ----------
# MAGIC %md ### 4. See the plan change (before vs after)

# COMMAND ----------
from sqlspark_optimizer.bench import executed_plan, join_ops
from sqlspark_optimizer.agents.translator import Translator

before = executed_plan(spark, Translator("spark").translate(sql).spark_sql)
after = executed_plan(spark, result.optimized_sql)
print("before joins:", join_ops(before))   # SortMergeJoin (or Photon variant)
print("after  joins:", join_ops(after))    # BroadcastHashJoin
