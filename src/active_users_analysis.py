# An active user is defined as a user who has performed at least one action
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_sub, current_timestamp, date_trunc, countDistinct, date_format, when

spark = SparkSession.builder.appName("DAU_MAU_Calculation").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Asia/Hong_Kong")

df = spark.read.csv("user_interactions_sample.csv", header=True, inferSchema=True)

df = df.dropDuplicates(["user_id", "timestamp"])

df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

df = df.filter(col("user_id").isNotNull() & col("timestamp").isNotNull())
df = df.filter(col("action_type").isin(["create", "edit", "share", "page_view", "delete"]))
df = df.filter(col("timestamp") <= current_timestamp())

#Outliers
quantiles = df.approxQuantile("duration_ms", [0.25, 0.75], 0.05)
q1, q3 = quantiles[0], quantiles[1]
iqr = q3 - q1
df = df.filter((col("duration_ms") >= q1 - 1.5 * iqr) & (col("duration_ms") <= q3 + 1.5 * iqr))

df = df.select("user_id", "timestamp")

df_dau = df.groupBy(date_format("timestamp", "yyyy-MM-dd").alias("date")) \
           .agg(countDistinct("user_id").alias("dau"))

df_dau.orderBy("date").show()

df_mau = df.groupBy(date_trunc("month", "timestamp").alias("month")) \
           .agg(countDistinct("user_id").alias("mau"))

df_mau.orderBy("month").show()

spark.stop()