from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import time

# Spark session

spark = SparkSession.builder \
    .appName("VehicleTelemetryStreaming") \
    .master("spark://spark:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema
schema = StructType() \
    .add("vehicle_id", StringType()) \
    .add("trip_id", StringType()) \
    .add("speed", IntegerType()) \
    .add("engine_temp", IntegerType()) \
    .add("fuel_level", IntegerType()) \
    .add("odometer", IntegerType()) \
    .add("location", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "vehicle_telemetry") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Basic processing
processed_df = parsed_df.withColumn(
    "status",
    col("engine_temp") > 120
)

print("Streaming job started...")

# query = processed_df.writeStream \
#     .format("console") \
#     .option("checkpointLocation", "/tmp/debug_console") \
#     .trigger(processingTime="5 seconds") \
#     .start()

# query.awaitTermination()

# processed_df = processed_df.coalesce(1)

def write_to_parquet(batch_df, batch_id):
    print(f"Writing batch {batch_id}")
    
    batch_df.write \
        .mode("append") \
        .parquet("/app/data/vehicle_telemetry")

query = processed_df.writeStream \
    .foreachBatch(write_to_parquet) \
    .option("checkpointLocation", "/app/data/checkpoints_final") \
    .trigger(processingTime="5 seconds") \
    .start()

# 👇 Graceful shutdown loop
try:
    while True:
        time.sleep(5)
except KeyboardInterrupt:
    print("Stopping stream gracefully...")
    query.stop()
    print("Stream stopped cleanly.")

# query = processed_df.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "/app/dataset/vehicle_telemetry_new") \
#     .option("checkpointLocation", "/app/dataset/checkpoints_new") \
#     .start()

# query.awaitTermination()