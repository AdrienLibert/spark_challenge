from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, countDistinct, lit, first, to_timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime
import pytz

spark = SparkSession.builder \
    .appName("appMetrics") \
    .master("local[*]") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "./spark-events") \
    .getOrCreate()

csv_file_path = "./data/user_interactions_sample.csv"

parquet_output_path = "./data/user_interactions_sample.parquet"

df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv(csv_file_path)

df.write.mode("overwrite").parquet(parquet_output_path)

spark.stop()