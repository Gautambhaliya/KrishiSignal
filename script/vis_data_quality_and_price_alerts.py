
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_date, lag, round, when, abs as spark_abs
)
from pyspark.sql.window import Window

# Configuration
INPUT_PATH = (
    "hdfs://localhost:9000/"
    "krishisignal/raw/mandi_prices/mandi_sample.csv"
)
VALIDATED_PATH = (
    "hdfs://localhost:9000/"
    "krishisignal/processed/validated_mandi_prices"
)
ALERTS_PATH = (
    "hdfs://localhost:9000/"
    "krishisignal/analytics/price_alerts"
)

PRICE_CHANGE_THRESHOLD = 3.0  # Percentage; configurable

spark = (
    SparkSession.builder
    .appName("KrishiSignal-DataQuality-PriceAlerts")
    .master("local[2]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    # 1. Read CSV as strings to validate values explicitly
    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(INPUT_PATH)
    )

    print("\n=== RAW DATA ===")
    raw_df.show(truncate=False)

    raw_count = raw_df.count()
    print(f"Total input records: {raw_count}")

    # 2. Normalize text and parse date and price
    normalized_df = raw_df.select(
        to_date(trim(col("date")), "yyyy-MM-dd").alias("date"),
        trim(col("district")).alias("district"),
        trim(col("market")).alias("market"),
        trim(col("crop")).alias("crop"),
        col("modal_price").cast("double").alias("modal_price"),
        col("date").alias("_original_date"),
        col("modal_price").alias("_original_price")
    )

    # Count rows with missing or invalid required fields
    invalid_condition = (
        col("date").isNull()
        | col("district").isNull()
        | (trim(col("district")) == "")
        | col("market").isNull()
        | (trim(col("market")) == "")
        | col("crop").isNull()
        | (trim(col("crop")) == "")
        | col("modal_price").isNull()
        | (col("modal_price") <= 0)
    )

    invalid_count = normalized_df.filter(invalid_condition).count()

    # 3. Keep valid rows and remove duplicate business records
    valid_df = normalized_df.filter(~invalid_condition).select(
        "date", "district", "market", "crop", "modal_price"
    )

    valid_before_dedup = valid_df.count()

    deduplicated_df = valid_df.dropDuplicates(
        ["date", "district", "market", "crop", "modal_price"]
    )

    duplicate_count = valid_before_dedup - deduplicated_df.count()

    print("\n=== DATA QUALITY REPORT ===")
    print(f"Input records: {raw_count}")
    print(f"Invalid records: {invalid_count}")
    print(f"Duplicate records removed: {duplicate_count}")
    print(f"Valid unique records: {deduplicated_df.count()}")

    # 4. Save validated data in HDFS
    (
        deduplicated_df.write
        .mode("overwrite")
        .parquet(VALIDATED_PATH)
    )

    print(f"\nValidated data saved to: {VALIDATED_PATH}")

    # 5. Compare each crop/district with its previous recorded price
    window_spec = (
        Window.partitionBy("crop", "district")
        .orderBy("date")
    )

    price_changes = (
        deduplicated_df
        .withColumn("previous_price", lag("modal_price").over(window_spec))
        .withColumn(
            "change_percent",
            round(
                (
                    (col("modal_price") - col("previous_price"))
                    / col("previous_price")
                ) * 100,
                2
            )
        )
        .withColumn(
            "alert_type",
            when(
                col("change_percent") >= PRICE_CHANGE_THRESHOLD,
                "PRICE_INCREASE"
            ).when(
                col("change_percent") <= -PRICE_CHANGE_THRESHOLD,
                "PRICE_DECREASE"
            )
        )
    )

    print("\n=== PRICE CHANGE ANALYSIS ===")
    price_changes.select(
        "date", "crop", "district", "modal_price",
        "previous_price", "change_percent", "alert_type"
    ).orderBy("crop", "district", "date").show(truncate=False)

    # 6. Keep significant changes for review
    alerts_df = price_changes.filter(
        spark_abs(col("change_percent")) >= PRICE_CHANGE_THRESHOLD
    )

    print("\n=== PRICE ALERTS (REQUIRING REVIEW) ===")
    alerts_df.select(
        "date", "crop", "district", "previous_price",
        "modal_price", "change_percent", "alert_type"
    ).show(truncate=False)

    (
        alerts_df.write
        .mode("overwrite")
        .parquet(ALERTS_PATH)
    )

    print(f"Price alerts saved to: {ALERTS_PATH}")

finally:
    spark.stop()
