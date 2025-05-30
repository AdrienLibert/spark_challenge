# Create Spark Jobs to Analyze CSV Files

## Description

Multiple jobs are implemented in PySpark 3.5.2 to analyze CSV files and merge data.

- `generate_data.py` generates CSV files with over 10 million rows for interactions and 1 million rows for metadata.
- `csv_to_parquet.py` One job converts CSV files to Parquet format.
- `spark_metrics_optimized.py` One job analyzes metrics, including the number of users per month and day, session duration, etc.
- `spark_merge.py` One job merges CSV files using a broadcast join.
- All jobs are containerized in a Dockerfile.
- A separate Dockerfile creates a Spark service to analyze jobs with the Spark UI.
- Everything is managed by a Docker Compose configuration.