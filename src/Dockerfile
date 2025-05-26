FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir pyspark==3.5.2

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/root/src"

WORKDIR /root
COPY src /root/src
RUN mkdir -p /root/data

RUN echo '#!/bin/bash\n\
spark-submit --master local[*] /root/src/generate_data.py || exit 1\n\
spark-submit --master local[*] /root/src/csv_to_parquet.py || exit 1\n\
for script in /root/src/*.py; do\n\
  [[ "$script" != "/root/src/generate_data.py" && "$script" != "/root/src/csv_to_parquet.py" ]] && spark-submit --master local[*] "$script" || exit 1\n\
done' > /entrypoint.sh && \
chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]