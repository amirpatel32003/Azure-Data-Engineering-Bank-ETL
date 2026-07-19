from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, regexp_replace, trim
from pyspark.sql.types import IntegerType, FloatType, DateType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("BankingETL").getOrCreate()

base_path = "abfss://banking-data@stdbanketl2026.dfs.core.windows.net/"

print("=== Starting ETL Pipeline ===")

# Load Raw Data from Landing
customers_df = spark.read.option("header", True).csv(base_path + "landing/customers.csv")
loans_df = spark.read.option("header", True).csv(base_path + "landing/loans.csv")
transactions_df = spark.read.option("header", True).csv(base_path + "landing/transactions.csv")

print("Raw data loaded successfully!")

# Silver Layer - Cleaning & Validation
customers_silver = customers_df \
    .withColumn("CustomerID", col("CustomerID").cast(IntegerType())) \
    .filter(col("CustomerID").isNotNull()) \
    .dropDuplicates(["CustomerID"]) \
    .withColumn("FirstName", trim(regexp_replace(col("FirstName"), r'[^a-zA-Z]', ''))) \
    .withColumn("LastName", trim(regexp_replace(col("LastName"), r'[^a-zA-Z]', ''))) \
    .withColumn("Province", when(col("Province").isNull(), lit("Unknown")).otherwise(col("Province"))) \
    .withColumn("CreditScore", col("CreditScore").cast(IntegerType())) \
    .filter((col("CreditScore") <= 850) & (col("CreditScore") >= 300)) \
    .withColumn("Age", col("Age").cast(IntegerType())) \
    .filter(col("Age") >= 18) \
    .withColumn("ingestion_timestamp", current_timestamp())

loans_silver = loans_df \
    .withColumn("CustomerID", col("CustomerID").cast(IntegerType())) \
    .filter(col("CustomerID").isNotNull()) \
    .dropDuplicates(["LoanID"]) \
    .withColumn("LoanAmount", col("LoanAmount").cast(FloatType())) \
    .filter(col("LoanAmount") > 0) \
    .withColumn("ApprovalDate", F.to_date(col("ApprovalDate"))) \
    .withColumn("Default", when(col("Default").isNull(), lit(False)).otherwise(col("Default").cast("boolean"))) \
    .withColumn("ingestion_timestamp", current_timestamp())

transactions_silver = transactions_df \
    .withColumn("CustomerID", col("CustomerID").cast(IntegerType())) \
    .filter(col("CustomerID").isNotNull()) \
    .dropDuplicates(["TransactionID"]) \
    .withColumn("Amount", col("Amount").cast(FloatType())) \
    .filter(col("Amount") > 0) \
    .withColumn("TransactionDate", F.to_date(col("TransactionDate"))) \
    .withColumn("ingestion_timestamp", current_timestamp())

# Write Silver Delta Tables
customers_silver.write.format("delta").mode("overwrite").save(base_path + "silver/customers")
loans_silver.write.format("delta").mode("overwrite").save(base_path + "silver/loans")
transactions_silver.write.format("delta").mode("overwrite").save(base_path + "silver/transactions")

print("Silver Layer Completed!")

# Gold Layer
gold_df = customers_silver.alias("c").join(loans_silver.alias("l"), "CustomerID", "left") \
    .join(transactions_silver.alias("t"), "CustomerID", "left") \
    .groupBy("c.CustomerID", "c.FirstName", "c.LastName", "c.Province", "c.CreditScore") \
    .agg(
        F.sum("l.LoanAmount").alias("TotalLoanAmount"),
        F.countDistinct("l.LoanID").alias("NumLoans"),
        F.avg("t.Amount").alias("AvgTransactionAmount"),
        F.max("l.Default").alias("HasDefaulted")
    ) \
    .withColumn("RiskLevel", when(col("CreditScore") > 750, lit("Low"))
                .when(col("CreditScore") > 650, lit("Medium"))
                .otherwise(lit("High")))

gold_df.write.format("delta").mode("overwrite").save(base_path + "gold/customer_analytics")

print("Gold Layer Completed! ETL Pipeline Finished Successfully.")
gold_df.show(10)
