# Session is a sequence of actions by the same user_id where the time gap between consecutive actions
# is less than or equal to 30 minutes
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, unix_timestamp, when, sum as sum_, count, avg
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("metrics_Calculation").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Asia/Hong_Kong")

df = spark.read.csv("user_interactions_sample.csv", header=True, inferSchema=True)
df = df.dropDuplicates(["user_id", "timestamp"])

df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

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
    count("*").alias("action_count")
)

overall_metrics = session_metrics.agg(
    avg("session_duration_ms").alias("avg_session_duration_ms"),
    avg("action_count").alias("avg_actions_per_session")
)

session_metrics.show(truncate=False)

overall_metrics.show(truncate=False)

spark.stop()