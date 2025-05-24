from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, date_format, count, countDistinct, to_timestamp
from pyspark.sql.functions import (
    col, to_timestamp, lag, unix_timestamp, current_timestamp,
    date_trunc, countDistinct, date_format, when, sum as sum_, count, avg
)

spark = SparkSession.builder \
    .appName("appMetrics") \
    .master("local[*]") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "./spark-events") \
    .getOrCreate()

parquet_path = "./data/user_interactions_sample.parquet"

df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .parquet(parquet_path)

df.printSchema()
df.show(5, truncate=False)

df = df.filter(col("timestamp") <= current_timestamp())

quantiles = df.approxQuantile("duration_ms", [0.25, 0.75], 0.05)
q1, q3 = quantiles[0], quantiles[1]
iqr = q3 - q1
df = df.filter((col("duration_ms") >= q1 - 1.5 * iqr) & (col("duration_ms") <= q3 + 1.5 * iqr))

df = df.select("user_id", "timestamp", "duration_ms", "action_type")

df_dau = df.groupBy(date_format("timestamp", "yyyy-MM-dd").alias("date")) \
           .agg(countDistinct("user_id").alias("dau"))

df_mau = df.groupBy(date_trunc("month", "timestamp").alias("month")) \
           .agg(countDistinct("user_id").alias("mau"))

window_spec = Window.partitionBy("user_id").orderBy("timestamp")
df = df.withColumn("prev_timestamp", lag("timestamp").over(window_spec))
df = df.withColumn(
    "time_diff_secs",
    when(
        col("prev_timestamp").isNotNull(),
        unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
    ).otherwise(0)
)
df = df.withColumn(
    "is_new_session",
    when(col("time_diff_secs") > 1800, 1).otherwise(0)
)
df = df.withColumn(
    "session_id",
    sum_("is_new_session").over(window_spec)
)

session_metrics = df.groupBy("user_id", "session_id").agg(
    sum_("duration_ms").alias("session_duration_ms"),
    count("action_type").alias("action_count")
)

overall_metrics = session_metrics.agg(
    avg("session_duration_ms").alias("avg_session_duration_ms"),
    avg("action_count").alias("avg_actions_per_session")
)

df_dau.orderBy("date").show()
df_mau.orderBy("month").show()
session_metrics.show(truncate=False)
overall_metrics.show(truncate=False)