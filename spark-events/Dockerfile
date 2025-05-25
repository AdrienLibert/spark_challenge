FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.2-bin-hadoop3.tgz && \
    mv spark-3.5.2-bin-hadoop3 /opt/spark && \
    rm spark-3.5.2-bin-hadoop3.tgz

WORKDIR /data

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN mkdir /spark-events && \
    chmod -R 777 /spark-events

COPY ./spark-events/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY ./spark-events /spark-events

CMD $SPARK_HOME/sbin/start-history-server.sh && tail -f /dev/null