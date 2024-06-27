# Databricks notebook source
# Databricks notebook source
# Required imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum as _sum, count as _count, month, year, avg as _avg
from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType, ArrayType, StructType, StructField


# Vulnerability Issue Should be Fixed before Push Code to Git hub
access_key1 = 'P+wSfElx34Z/H120WdB/'
access_key2= '0/rF8dD+TVSY2H43sRPTWJqimAWY94Xkd23HXjt2eSSr0EYdVW/XHuqB+AStEbOvNA=='
access_key = access_key1 + access_key2
print(access_key)

# Specify the container and account name
container_name = "continer1"
account_name = "gitpush"

# Mount point in DBFS
mount_point = "/mnt/adls"

# Check if the directory is already mounted
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    # Mount ADLS Gen2 using access key variable
    dbutils.fs.mount(
      source=f"wasbs://{container_name}@{account_name}.blob.core.windows.net",
      mount_point=mount_point,
      extra_configs={f"fs.azure.account.key.{account_name}.blob.core.windows.net": access_key}
    )


# List the mounted directories to verify
display(dbutils.fs.ls(mount_point))

# Path to the multi-line JSON file in the mounted storage
json_file_path = f'{mount_point}/inbount/shield.json'


# Read the multi-line JSON file into a DataFrame with multiLine option set to true
df = spark.read.option("multiLine", True).json(json_file_path)

flattened_df = df.withColumn("sale_id", col("sale.sale_id").cast(IntegerType())) \
                 .withColumn("sale_date", col("sale.sale_date").cast(StringType())) \
                 .withColumn("final_premium_amt", col("sale.final_premium_amt").cast(IntegerType())) \
                 .withColumn("sales_mode", col("sale.sales_mode").cast(StringType())) \
                 .withColumn("revenue_id", col("revenue.revenue_id").cast(IntegerType())) \
                 .withColumn("revenue_date", col("revenue.revenue_date").cast(StringType())) \
                 .withColumn("revenue_amt", col("revenue.revenue_amt").cast(IntegerType())) \
                 .withColumn("customer_code", col("customer_code").cast(IntegerType())) \
                 .withColumn("age", col("age").cast(IntegerType())) \
                 .withColumn("age_group", col("age_group").cast(StringType())) \
                 .withColumn("city", col("city").cast(StringType())) \
                 .withColumn("acquisition_channel", col("acquisition_channel").cast(StringType())) \
                 .withColumn("policy_id", col("policy_id").cast(StringType())) \
                 .withColumn("base_coverage_amt", col("base_coverage_amt").cast(IntegerType())) \
                 .withColumn("base_premium_amt", col("base_premium_amt").cast(IntegerType())) \
                 .withColumn("date", col("date").cast(StringType())) \
                 .withColumn("day", col("day").cast(IntegerType())) \
                 .withColumn("month", col("month").cast(IntegerType())) \
                 .withColumn("year", col("year").cast(IntegerType())) \
                 .withColumn("day_type", col("day_type").cast(StringType())) \
                 .withColumn("claims", col("claims").cast(ArrayType(IntegerType()))) \
                 .drop("sale", "revenue")  # Drop the original nested columns

flattened_df.show(truncate=False)

# Explode the claims array to calculate KPIs
exploded_df = flattened_df.withColumn("claim_amount", explode("claims")).drop("claims")

exploded_df.show(truncate=False)


# Path to save the CSV file
csv_file_path = f'{mount_point}/output/shield.csv'

# Write the DataFrame to a CSV file
exploded_df.write.csv(csv_file_path, header=True)

# Verify the CSV file is written
display(dbutils.fs.ls(f'{mount_point}/output'))

