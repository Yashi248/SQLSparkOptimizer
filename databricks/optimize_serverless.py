# Databricks notebook source
# MAGIC %md
# MAGIC # SQLSpark Optimizer on Databricks (Serverless / Free Edition)
# MAGIC Serverless locks down Spark configs, so we can't force the broadcast
# MAGIC anti-pattern. Instead we use the **predicate-pushdown** pattern, which needs
# MAGIC no config: a `WHERE YEAR(date) = 1994` filter is non-sargable and blocks
# MAGIC pushdown regardless of engine/config. The optimizer detects it, rewrites it
# MAGIC to a sargable range, and proves the output is identical.

# COMMAND ----------
# MAGIC %pip install --no-deps git+https://github.com/Yashi248/SQLSparkOptimizer.git
# MAGIC %pip install sqlglot langgraph

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ### Build a table with a date column

# COMMAND ----------
from pyspark.sql import functions as F

# ~5M rows, ship_date spread across 1992–1997, plus a price column.
spark.range(0, 5_000_000) \
    .withColumn("ship_date", F.date_add(F.lit("1992-01-01").cast("date"),
                                        (F.col("id") % 2000).cast("int"))) \
    .withColumn("price", (F.col("id") % 1000).cast("double")) \
    .createOrReplaceTempView("sales")

# COMMAND ----------
# MAGIC %md ### Optimize — the non-sargable YEAR() filter gets rewritten + validated

# COMMAND ----------
import os
os.environ["SQLSPARK_DISABLE_TELEMETRY"] = "1"  # skip MLflow on Databricks serverless
from sqlspark_optimizer import optimize

sql = "SELECT SUM(price) AS revenue FROM sales WHERE YEAR(ship_date) = 1994"

result = optimize(sql, spark, source_dialect="spark",
                  timing_runs=1, use_llm_explain=False)

print("applied rules :", result.applied_rules)     # ['sargable_year']
print("validated     :", result.status != "reverted")
print("explanation   :", result.explanation)
print("\noriginal :", sql)
print("optimized:", result.optimized_sql.replace("\n", " "))

# COMMAND ----------
# MAGIC %md ### Proof: the filter now pushes into the scan

# COMMAND ----------
from sqlspark_optimizer.bench import executed_plan, pushed_filters
from sqlspark_optimizer.agents.translator import Translator

before = executed_plan(spark, Translator("spark").translate(sql).spark_sql)
after = executed_plan(spark, result.optimized_sql)
print("PushedFilters BEFORE:", pushed_filters(before))   # YEAR() can't push
print("PushedFilters AFTER :", pushed_filters(after))    # range pushes into scan
