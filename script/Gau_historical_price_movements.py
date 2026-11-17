
# KrishiSignal: Find the biggest mandi price changes

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, lag
from pyspark.sql.window import Window

# 1. Start Spark
spark = (
    SparkSession.builder
    .appName("KrishiSignalPriceChanges")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Location of historical mandi data in HDFS
file_path = (
    "hdfs://localhost:9000/krishisignal/"
    "historical/mandi_prices/gujarat_mandi_historical_2026_07.csv"
)

# 3. Read the CSV file
data = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
)

# 4. Convert the important columns into correct data types
data = (
    data
    .withColumn("date", col("date").cast("date"))
    .withColumn("modal_price", col("modal_price").cast("double"))
)

# 5. Remove rows with missing or invalid dates and prices
data = data.filter(
    col("date").isNotNull()
    & col("modal_price").isNotNull()
    & (col("modal_price") > 0)
)

# 6. Calculate one average price per day and market/crop variety
daily_prices = (
    data
    .groupBy("date", "district", "market", "commodity", "variety")
    .agg(round(avg("modal_price"), 2).alias("price"))
)

# 7. Arrange prices by date for each market and crop variety
price_window = (
    Window
    .partitionBy("district", "market", "commodity", "variety")
    .orderBy("date")
)

# 8. Get the previous available price for comparison
results = daily_prices.withColumn(
    "previous_price",
    lag("price").over(price_window)
)

# 9. Calculate the price difference and percentage change
*

# 10. Keep only rows with valid current and previous prices
results = results.filter(
    col("date").isNotNull()
    & col("price").isNotNull()
    & col("previous_price").isNotNull()
    & (col("previous_price") > 0)
)

# Columns we want to display
columns = [
    "date", "district", "market", "commodity", "variety",
    "previous_price", "price", "price_change", "change_percent"
]

# 11. Show the 10 biggest percentage increases
print("\n===== TOP 10 PRICE INCREASES =====")

results.orderBy(
    col("change_percent").desc()
).select(*columns).show(10, truncate=False)

# 12. Show the 10 biggest percentage decreases
print("\n===== TOP 10 PRICE DECREASES =====")

results.orderBy(
    col("change_percent").asc()
).select(*columns).show(10, truncate=False)

# 13. Stop Spark
spark.stop()
