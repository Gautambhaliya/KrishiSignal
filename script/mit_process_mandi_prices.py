from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round

spark = (
    SparkSession.builder
    .appName("KrishiSignal-Mandi-ETL")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = "hdfs://localhost:9000/krishisignal/raw/mandi_prices/mandi_sample.csv"
output_path = "hdfs://localhost:9000/krishisignal/processed/mandi_price_summary"

# Read the raw CSV
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("=== Raw Mandi Data ===")
df.show(truncate=False)
df.printSchema()

# Basic data-quality checks
clean_df = df.dropna(
    subset=["date", "district", "market", "crop", "modal_price"]
)

clean_df = clean_df.filter(clean_df.modal_price > 0)

# Calculate average price by crop and district
summary_df = (
    clean_df.groupBy("crop", "district")
    .agg(
        round(avg("modal_price"), 2).alias("average_price"),
        count("*").alias("record_count")
    )
)

print("=== Mandi Price Summary ===")
summary_df.show(truncate=False)

# Save processed output as Parquet
(
    summary_df.write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Processed data saved to: {output_path}")

spark.stop()
