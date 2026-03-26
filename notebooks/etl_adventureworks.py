from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize Spark
spark = SparkSession.builder.appName("AdventureWorksETL").getOrCreate()

# -------------------------------
# 🔹 Load Data (Bronze Layer)
# -------------------------------

sales_df = spark.read.csv("/mnt/bronze/AdventureWorks_Sales_2017.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("/mnt/bronze/AdventureWorks_Customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv("/mnt/bronze/AdventureWorks_Products.csv", header=True, inferSchema=True)

# -------------------------------
# 🔹 Data Cleaning
# -------------------------------

sales_df = sales_df.dropna().dropDuplicates()
customers_df = customers_df.dropna().dropDuplicates()
products_df = products_df.dropna().dropDuplicates()

# -------------------------------
# 🔹 Transformations (Silver Layer)
# -------------------------------

# Join datasets
df_joined = sales_df.join(customers_df, "CustomerKey", "inner") \
                    .join(products_df, "ProductKey", "inner")

# Create total sales column
df_transformed = df_joined.withColumn(
    "TotalSales",
    col("OrderQuantity") * col("UnitPrice")
)

# Save cleaned data
df_transformed.write.mode("overwrite").parquet("/mnt/silver/sales_transformed")

# -------------------------------
# 🔹 Aggregation (Gold Layer)
# -------------------------------

df_gold = df_transformed.groupBy("ProductName") \
    .agg(sum("TotalSales").alias("TotalRevenue"))

# Save final data
df_gold.write.mode("overwrite").parquet("/mnt/gold/sales_summary")

# -------------------------------
# ✅ Done
# -------------------------------
print("ETL Pipeline Executed Successfully!")