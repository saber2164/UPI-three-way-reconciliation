FROM flink:1.19-scala_2.12-java11
ENV FLINK_HOME=/opt/flink
RUN mkdir -p $FLINK_HOME/plugins/s3-fs-hadoop && cp $FLINK_HOME/opt/flink-s3-fs-hadoop-1.19.*.jar $FLINK_HOME/plugins/s3-fs-hadoop/
# Connectors: Kafka, Avro + Schema Registry, Iceberg, Postgres JDBC.
# `curl -f` is deliberate: without it a 404 becomes a 554-byte HTML file that
# only fails later as "Could not find any factory for identifier 'iceberg'".
RUN set -eux; cd $FLINK_HOME/lib && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.19.1/flink-sql-avro-confluent-registry-1.19.1.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar && \
    test -s iceberg-flink-runtime-1.19-1.6.1.jar
# Pre-create the checkpoint directory so a fresh named volume inherits the
# ownership of the `flink` user instead of being created root-owned, which
# makes every job fail with "Failed to create directory for shared state".
RUN mkdir -p $FLINK_HOME/checkpoints && chown -R flink:flink $FLINK_HOME/checkpoints
