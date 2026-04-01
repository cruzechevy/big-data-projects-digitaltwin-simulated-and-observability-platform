from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, count, countDistinct
from pyspark.sql.types import StructType, StringType, IntegerType
import time

# ==============================
# Spark Session
# ==============================
#.master("spark://spark:7077") \
spark = SparkSession.builder \
    .appName("VehicleTelemetryStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==============================
# Schema
# ==============================
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

# ==============================
# Read from Kafka
# ==============================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "vehicle_telemetry") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# ==============================
# Parse JSON
# ==============================
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ==============================
# Basic Processing (Digital Twin Logic)
# ==============================
processed_df = parsed_df.withColumn(
    "engine_alert",
    when(col("engine_temp") > 120, "HIGH").otherwise("NORMAL")
)

print("🚀 Streaming job started...")

# ==============================
# Observability + Write Function
# ==============================
def write_to_parquet(batch_df, batch_id):
    start_time = time.time()
    print(f"\n📦 Processing batch {batch_id}")
    print(f"Count: {batch_df.count()}")
    # ----------------------------
    # 1. Write Raw Data
    # ----------------------------
    batch_df.coalesce(1).write \
        .mode("append") \
        .parquet("/app/data/vehicle_telemetry")


    # ----------------------------
    # 2. Data Quality Metrics
    # ----------------------------
    dq_metrics = batch_df.groupBy().agg(
        count("*").alias("total_records"),
        count(when(col("engine_temp").isNull(), True)).alias("null_engine_temp"),
        count(when(col("engine_temp") > 250, True)).alias("high_engine_temp"),
        count(when(col("speed") > 150, True)).alias("overspeed_count")
    )

    metrics = dq_metrics.collect()[0]

    total = metrics["total_records"]
    nulls = metrics["null_engine_temp"]
    high_temp = metrics["high_engine_temp"]
    overspeed = metrics["overspeed_count"]

    print("📊 Metrics:", metrics)

    # ----------------------------
    # 3. ALERT LOGIC
    # ----------------------------
    if high_temp > 0:
        print("🚨 ALERT: High engine temperature detected!")

    if nulls > 0:
        print("⚠️ WARNING: Null engine_temp values present!")

    if overspeed > 5:
        print("🚨 ALERT: Multiple overspeed events!")

    # ----------------------------
    # Active Vehicles by City
    # ----------------------------
    active_vehicles = batch_df.filter(
        col("event_type") == "RUNNING"
    ).groupBy("location").agg(
        countDistinct("vehicle_id").alias("active_vehicle_count")
    )

    print("🚗 Active Vehicles by City:")
    active_vehicles.show(truncate=False)

    # Write to separate folder
    active_vehicles.write \
        .mode("append") \
        .parquet("/app/data/vehicle_activity")

    # ----------------------------
    # 5. Write Observability
    # ----------------------------
    dq_metrics.write \
        .mode("append") \
        .parquet("/app/data/observability")
    
    # ----------------------------
    # Pipeline Health
    # ----------------------------
    end_time = time.time()
    duration = round(end_time - start_time, 2)

    print(f"⏱️ Batch Processing Time: {duration} seconds")

    if duration > 5:
        print("⚠️ WARNING: Processing delay detected!")

    

# ==============================
# Streaming Query
# ==============================
query = processed_df.writeStream \
    .foreachBatch(write_to_parquet) \
    .option("checkpointLocation", "/app/data/checkpoints_vehicle_telemetry") \
    .trigger(processingTime="5 seconds") \
    .start()

# ==============================
# Graceful Shutdown
# ==============================
try:
    while True:
        time.sleep(5)
except KeyboardInterrupt:
    print("🛑 Stopping stream gracefully...")
    query.stop()
    print("✅ Stream stopped cleanly.")