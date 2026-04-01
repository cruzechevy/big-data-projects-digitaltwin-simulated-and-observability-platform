FROM apache/spark:3.5.0

USER root

WORKDIR /app

# Copy project files
COPY . /app

# Install any Python deps if needed
RUN pip install kafka-python

#CMD ["/opt/spark/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.jars.ivy=/tmp/.ivy2 /app/services/spark/streaming_job.py"]
# Run Spark job
#CMD sleep 15 && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.jars.ivy=/tmp/.ivy2 /app/services/spark/streaming_job.py