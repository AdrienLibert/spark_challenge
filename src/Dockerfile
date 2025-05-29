FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir pyspark==3.5.2

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/app/src"

ENV SPARK_EVENT_LOG_DIR="/app/spark-events"
ENV SPARK_EVENT_LOG_ENABLED="true"
ENV SPARK_LOCAL_DIRS="/app/spark-events"

WORKDIR /app

COPY src /app/src

RUN mkdir -p /app/data
RUN mkdir -p /app/spark-events

RUN echo '#!/bin/bash\n\
set -x\n\
echo "Starting entrypoint.sh at $(date)"\n\
echo "Running generate_data.py"\n\
python /app/src/generate_data.py > /app/data/generate_data.log 2>&1 || { echo "generate_data.py failed"; exit 1; }\n\
echo "Running csv_to_parquet.py"\n\
python /app/src/csv_to_parquet.py > /app/data/csv_to_parquet.log 2>&1 || { echo "csv_to_parquet.py failed"; exit 1; }\n\
echo "Running other Python scripts in /app/src"\n\
for script in /app/src/*.py; do\n\
  if [[ "$script" != "/app/src/generate_data.py" && "$script" != "/app/src/csv_to_parquet.py" ]]; then\n\
    echo "Running $script"\n\
    python "$script" > /app/data/$(basename "$script").log 2>&1 || { echo "$script failed"; exit 1; }\n\
  fi\n\
done\n\
echo "All scripts completed successfully at $(date)"' > /app/src/entrypoint.sh && \
chmod +x /app/src/entrypoint.sh

CMD ["/app/src/entrypoint.sh"]