from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    avg,
    min as spark_min,
    max as spark_max,
    round as spark_round
)

spark = (
    SparkSession.builder
    .appName("KrishiSignal-Historical-Mandi-Analysis")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = (
    "hdfs://localhost:9000/krishisignal/"
    "historical/mandi_prices/gujarat_mandi_historical_2026_07.csv"
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

df = (
    df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("modal_price", col("modal_price").cast("double"))
    .withColumn("min_price", col("min_price").cast("double"))
    .withColumn("max_price", col("max_price").cast("double"))
)

print("\n========== DATA SCHEMA ==========")
df.printSchema()

print("\n========== BASIC DATA SUMMARY ==========")
df.select(
    count("*").alias("total_records"),
    spark_min("date").alias("first_date"),
    spark_max("date").alias("last_date"),
    count("modal_price").alias("valid_modal_prices")
).show(truncate=False)

print("\n========== TOP 15 CROPS BY RECORD COUNT ==========")
(
    df.groupBy("commodity")
    .agg(
        count("*").alias("record_count"),
        spark_round(avg("modal_price"), 2).alias("average_modal_price"),
        spark_min("modal_price").alias("minimum_modal_price"),
        spark_max("modal_price").alias("maximum_modal_price")
    )
    .orderBy(col("record_count").desc())
    .show(15, truncate=False)
)

print("\n========== DAILY PRICE HISTORY: TOMATO ==========")
(
    df.filter(col("commodity").ilike("%Tomato%"))
    .groupBy("date")
    .agg(
        count("*").alias("market_records"),
        spark_round(avg("modal_price"), 2).alias("average_modal_price"),
        spark_min("modal_price").alias("minimum_modal_price"),
        spark_max("modal_price").alias("maximum_modal_price")
    )
    .orderBy("date")
    .show(30, truncate=False)
)

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, round as spark_round

print("\n========== MARKET-WISE PRICE CHANGE ==========")

# Compare the same crop, market and variety over time.
window_spec = (
    Window
    .partitionBy("district", "market", "commodity", "variety")
    .orderBy("date")
)

price_history = (
    df.groupBy(
        "date", "district", "market", "commodity", "variety"
    )
    .agg(
        spark_round(avg("modal_price"), 2).alias("modal_price")
    )
)

price_changes = (
    price_history
    .withColumn(
        "previous_price",
        lag("modal_price").over(window_spec)
    )
    .withColumn(
        "price_change",
        spark_round(col("modal_price") - col("previous_price"), 2)
    )
    .withColumn(
        "change_percent",
        spark_round(
            (col("modal_price") - col("previous_price"))
            / col("previous_price") * 100,
            2
        )
    )
)

(
    price_changes
    .filter(col("previous_price").isNotNull())
    .orderBy(col("date"), col("commodity"))
    .select(
        "date", "district", "market", "commodity", "variety",
        "previous_price", "modal_price",
        "price_change", "change_percent"
    )
    .show(30, truncate=False)
)

spark.stop()
