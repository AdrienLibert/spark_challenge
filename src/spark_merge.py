from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,broadcast
)

spark = SparkSession.builder \
    .appName("appMetricsJoin") \
    .master("local[*]") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "./spark-events") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

interactions_parquet_path = "./data/user_interactions_sample.parquet"
metadata_parquet_path = "./data/user_metadata_sample.parquet"

#Read data
df_interactions = spark.read.option("header", "true") \
                          .option("inferSchema", "true") \
                          .parquet(interactions_parquet_path)
df_metadata = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .parquet(metadata_parquet_path)

join_column = "user_id"

skewed_user_counts_interactions = df_interactions.groupBy(join_column).count().orderBy(col("count").desc()).limit(10)
print("\nTop 10 most frequent user_ids in interactions:")
skewed_user_counts_interactions.show()
skewed_user_counts_interactions = df_interactions.groupBy(join_column).count().orderBy(col("count").asc()).limit(10)
print("\nTop 10 least frequent user_ids in interactions:")
skewed_user_counts_interactions.show()


skewed_user_counts_metadata = df_metadata.groupBy(join_column).count().orderBy(col("count").desc()).limit(10)
print("\nTop 10 most frequent user_ids in metadata:")
skewed_user_counts_metadata.show()
skewed_user_counts_metadata = df_metadata.groupBy(join_column).count().orderBy(col("count").asc()).limit(10)
print("\nTop 10 least frequent user_ids in interactions:")
skewed_user_counts_metadata.show()

n_interactions = df_interactions.count()
n_metadata = df_metadata.count()

print(n_metadata)
print(n_interactions)
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))


#Use broadcast join because the number of rows in metadata is much smaller than interactions and the memory threshold is set to 10MB
df_joined = df_interactions.join(broadcast(df_metadata), join_column, "inner")

df_joined.explain()

df_joined.show()
spark.stop()