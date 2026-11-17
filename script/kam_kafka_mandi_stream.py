
# Import Spark tools
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------

# Kafka server and topic
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "mandi-prices"

# HDFS folder for processed mandi data
OUTPUT_PATH = (
    "hdfs://localhost:9000/krishisignal/processed/streaming_mandi_prices_api_v2"
)

# Checkpoint folder helps Spark recover streaming progress
CHECKPOINT_PATH = (
    "hdfs://localhost:9000/krishisignal/checkpoints/mandi_price_stream_recovery_api_v2"
)

# --------------------------------------------------
# 2. Start Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("KrishiSignalMandiStream")
    .master("local[2]")
    # Kafka connector must match the installed Spark version
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
    )
    .getOrCreate()
)

# Show only warning and error messages
spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 3. Define the structure of each Kafka JSON record
# --------------------------------------------------

schema = StructType([
    StructField("date", StringType(), True),
    StructField("district", StringType(), True),
    StructField("market", StringType(), True),
    StructField("crop", StringType(), True),
    StructField("modal_price", DoubleType(), True)
])

try:
    # --------------------------------------------------
    # 4. Read messages from Kafka
    # --------------------------------------------------

    kafka_data = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)#Select
        .option("startingOffsets", "earliest")#Req.new Rec
        .load()
    )

    # Kafka message values are bytes; convert them to text
    json_data = kafka_data.select(
        col("value").cast("string").alias("json")
    )

    # --------------------------------------------------
    # 5. Convert JSON text into columns
    # --------------------------------------------------

    parsed_data = (
        json_data
        .select(from_json(col("json"), schema).alias("data"))
        .select("data.*")
    )

    # Convert the date into a Spark date and add processing time
    parsed_data = (
        parsed_data
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .withColumn("processed_at", current_timestamp())
    )

    # --------------------------------------------------
    # 6. Remove invalid records
    # --------------------------------------------------

    clean_data = parsed_data.filter(
        col("date").isNotNull()
        & col("district").isNotNull()
        & (col("district") != "")
        & col("market").isNotNull()
        & (col("market") != "")
        & col("crop").isNotNull()
        & (col("crop") != "")
        & col("modal_price").isNotNull()
        & (col("modal_price") > 0)
    )

    # --------------------------------------------------
    # 7. Write processed records to HDFS
    # --------------------------------------------------

    stream = (
        clean_data.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print("KrishiSignal streaming is running. Press Ctrl+C to stop.")

    # Keep the streaming application running
    stream.awaitTermination()

except KeyboardInterrupt:
    print("Stopping KrishiSignal streaming...")

finally:
    # Stop Spark and release resources
    spark.stop()
