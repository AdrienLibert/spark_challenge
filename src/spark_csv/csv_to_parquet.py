from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSVtoParquet") \
    .master("local[*]") \
    .getOrCreate()

csv_path_data = "./data/user_interactions_sample.csv"
csv_path_metadata = "./data/user_metadata_sample.csv"

parquet_path_data = "./data/user_interactions_sample.parquet"
parquet_path_metadata = "./data/user_metadata_sample.parquet"

from_csv = spark.read.option("header", "true") \
                             .option("inferSchema", "true") \
                             .csv(csv_path_data)
from_csv.write.mode("overwrite").parquet(parquet_path_data)


from_csv = spark.read.option("header", "true") \
                             .option("inferSchema", "true") \
                             .csv(csv_path_metadata)
from_csv.write.mode("overwrite").parquet(parquet_path_metadata)


spark.stop()