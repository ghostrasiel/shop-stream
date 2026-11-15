FROM apache/spark:3.5.3

USER root

ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/job

COPY spark/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Pull the Kafka source connector at build time. Startup should not depend on Maven.
RUN mkdir -p /opt/ivy \
    && /opt/spark/bin/spark-submit \
        --master local[1] \
        --driver-memory 512m \
        --conf spark.jars.ivy=/opt/ivy \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        --class org.apache.spark.examples.SparkPi \
        /opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar 2 \
    && chmod -R a+rX /opt/ivy

COPY spark/job /opt/job
COPY spark/entrypoint.sh /opt/shop-entrypoint.sh
RUN chmod +x /opt/shop-entrypoint.sh

ENTRYPOINT ["/opt/shop-entrypoint.sh"]
