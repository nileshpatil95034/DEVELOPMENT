# Databricks notebook source
'''
File : Insurance_ETL.py
Description : 
Usage :
Contributore : sanco
Contact : 
Created : 28-06-2024
'''

# COMMAND ----------

# Databricks notebook source
# Required imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_date,regexp_replace,date_format, sum as _sum, count as _count, month, year, avg as _avg, sha2
from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType, ArrayType, StructType, StructField
import pandas as pd
import os

# COMMAND ----------

def encrypt_sensitive_fields(df, input_list_pii_field):
    for field in input_list_pii_field:
        encrypted_field_name = f"{field}_encrypted"
        df = df.withColumn(encrypted_field_name, sha2(col(field), 256).cast(StringType()))
    
    # Drop the original PII fields
    df = df.drop(*input_list_pii_field)
    
    return df

# COMMAND ----------

spark = SparkSession.builder\
        .appName('InsuranceCraft')\
        .config('Spark.sql.adaptive.enabled',True)\
        .config("Spark.dynamicAllocation.enables",True)\
        .getOrCreate()

#we can use following config for optimization
 #.config("Spark.serializer","org.apache.spark.serializer.KyroSerializer")\
       # .config("Spark.sql.autoBroadcastThreshold","10M")\
        #.config('spark.dynamicAllocation.minExecutors', "5")\
        #.config('spark.dynamicAllocation.maxExecutors', "30")\

# COMMAND ----------

# MAGIC %md
# MAGIC Mount The Notebook

# COMMAND ----------

# MAGIC %run /Workspace/Repos/akale220301@gmail.com/DEVELOPMENT/Mount_Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC Read file
# MAGIC

# COMMAND ----------

# Define the schema for the JSON data
schema = StructType([
    StructField("customer_code", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("age_group", StringType(), True),
    StructField("city", StringType(), True),
    StructField("acquisition_channel", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("base_coverage_amt", IntegerType(), True),
    StructField("base_premium_amt", IntegerType(), True),
    StructField("sale", StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("sale_date", StringType(), True),
        StructField("final_premium_amt", IntegerType(), True),
        StructField("sales_mode", StringType(), True)
    ]), True),
    StructField("revenue", StructType([
        StructField("revenue_id", IntegerType(), True),
        StructField("revenue_date", StringType(), True),
        StructField("revenue_amt", IntegerType(), True)
    ]), True),
    StructField("claims", ArrayType(IntegerType()), True),
    StructField("date", StringType(), True),
    StructField("day", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("day_type", StringType(), True),
    StructField("pii_fields", StructType([
        StructField("pan_number", StringType(), True),
        StructField("aadhaar_number", StringType(), True)
    ]), True)
])

# Define the JSON file path
json_file_path = f'{mount_point}/inbound/shield.json'

# Read the JSON file into a DataFrame with optimisation in cache
df = spark.read.option("multiline", "true").json(json_file_path, schema=schema).cache()

df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze Layer : Rwa Data and Checkpointing 1

# COMMAND ----------

################# Schema Transformations like Flatten the nested structure and define data types #######
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
                 .withColumn("pan_number", col("pii_fields.pan_number").cast(StringType())) \
                 .withColumn("aadhaar_number", col("pii_fields.aadhaar_number").cast(StringType())) \
                 .drop("sale", "revenue", "pii_fields")  

flattened_df.show(truncate=False)

# Explode the claims array to calculate KPIs
exploded_df = flattened_df.withColumn("claim_amount", explode("claims")).drop("claims")

### encrypt PII fields by UDF
input_list_pii_field = ["pan_number", "aadhaar_number"]
# Apply the function
exploded_df = encrypt_sensitive_fields(exploded_df, input_list_pii_field)
exploded_df.show(truncate=False)

### Write the Data into bronze layer
bronze_path = f'{mount_point}/outbound/bronze'
exploded_df.coalesce(1).write.format('csv').option('header', 'true').mode("overwrite").save(bronze_path)

### Rename the file and Delete unwanted file whcih are created
#  List the files in the directory to find the CSV file
files = dbutils.fs.ls(bronze_path)
# Filter the files to find the one that starts with "part-00000"
part_file = [file for file in files if file.name.startswith("part-00000") and file.name.endswith(".csv")]
# If the part file is found, move and rename it to bronze.csv
if part_file:
    dbutils.fs.mv(part_file[0].path, f"{bronze_path}/bronze.csv")
# Clean up any other files that were created during the write process (e.g., _SUCCESS file)
for file in files:
    if not file.name.endswith(".csv"):
        dbutils.fs.rm(file.path, True)
print("Rename Sucessfully in BRONZ")

### Checkpointing 1
# Set checkpoint directory
checkpoint_dir = f'{mount_point}/checkpoint'
spark.sparkContext.setCheckpointDir(checkpoint_dir)
# Checkpoint the DataFrame eagerly for 1st Transformation
exploded_df = exploded_df.checkpoint(eager=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Layer : Trabsform Data and Checkpointing 2

# COMMAND ----------

# Filter the Columns --> By Select Statement [ list of columnss]
columns = ["sale_id", "sale_date", "final_premium_amt", "sales_mode", "revenue_id", "revenue_date", 
           "revenue_amt", "customer_code", "age", "age_group", "city", "acquisition_channel", 
           "policy_id", "base_coverage_amt", "base_premium_amt", "date", "day", "month", 
           "year", "day_type", "claim_amount", "pan_number_encrypted", "aadhaar_number_encrypted"]

silverdf = exploded_df.select(*columns)

# Add New Columns --> By withColumn [as per requirement]
silverdf = silverdf.withColumn("etl_date", date_format(current_date(), "dd-MM-yyyy"))

# Handling nulls --> by fillna() & df.dropna()
silverdf = silverdf.fillna(0).fillna('')

# Handling Duplicate --> by dropDuplicates()
silverdf= silverdf.dropDuplicates()

# Removing Special Character &  --> by regex
silverdf = silverdf.withColumn("age", regexp_replace (col("age"), "[^a-zA-Z0-9 ]", " "))

### Write the Data into silver layer
silver_path = f'{mount_point}/outbound/silver'
silverdf.coalesce(1).write.format('csv').option('header', 'true').mode("overwrite").save(silver_path)

### Rename the file and Delete unwanted file whcih are created
# List the files in the directory to find the CSV file
files = dbutils.fs.ls(silver_path)
# Filter the files to find the one that starts with "part-00000"
part_file = [file for file in files if file.name.startswith("part-00000") and file.name.endswith(".csv")]
# If the part file is found, move and rename it to bronze.csv
if part_file:
    dbutils.fs.mv(part_file[0].path, f"{silver_path}/Silver.csv")
# Clean up any other files that were created during the write process (e.g., _SUCCESS file)
for file in files:
    if not file.name.endswith(".csv"):
        dbutils.fs.rm(file.path, True)
print("Rename Sucessfully in Silver")

### Checkpointing 2
# Checkpoint the DataFrame eagerly for 1st Transformation
silverdf = exploded_df.checkpoint(eager=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer : Complex Transformation for Metric key & KPI and Checkpointing 2

# COMMAND ----------

# Calculate KPIs: Total claims per policy, total claim amount, and average claim amount
total_claims_per_policy = silverdf.groupBy("policy_id").count().withColumnRenamed("count", "total_claims")
total_claims_per_policy.show(truncate=False)

total_claim_amount_per_policy = silverdf.groupBy("policy_id").agg(_sum("claim_amount").alias("total_claim_amount"))
total_claim_amount_per_policy.show(truncate=False)

avg_claim_amount_per_policy = silverdf.groupBy("policy_id").agg(_avg("claim_amount").alias("avg_claim_amount"))
avg_claim_amount_per_policy.show(truncate=False)

# Join KPIs with original DataFrame
transformed_df = silverdf.join(total_claims_per_policy, on="policy_id", how="left") \
                   .join(total_claim_amount_per_policy, on="policy_id", how="left") \
                   .join(avg_claim_amount_per_policy, on="policy_id", how="left")

transformed_df.show()

# Total Customers
total_customers = silverdf.select("customer_code").distinct().count()
print(f"Total Customers: The insurance company has a customer base of {total_customers}")

# Customer Distribution by Age Group
age_group_distribution = silverdf.groupBy("age_group").agg(_count("customer_code").alias("customer_count"))
age_group_distribution.show()

# Customer Acquisition Channels
acquisition_channel_distribution = silverdf.groupBy("acquisition_channel").agg(_count("customer_code").alias("customer_count"))
acquisition_channel_distribution.show()

# Customer Distribution by City
city_distribution = silverdf.groupBy("city").agg(_count("customer_code").alias("customer_count"))

# Customer Retention Rate (assuming previous data)
# Calculate retention rate for March
monthly_revenue_customers = exploded_df.groupBy(year("sale_date").alias("year"), month("sale_date").alias("month")) \
                              .agg(_sum("revenue_amt").alias("total_revenue"), _count("customer_code").alias("total_customers"))

previous_month_data = monthly_revenue_customers.filter((col("year") == 2023) & (col("month") == 2)).first()
previous_month_customers = previous_month_data["total_customers"]

march_data = monthly_revenue_customers.filter((col("year") == 2023) & (col("month") == 3)).first()

if march_data is not None:
    march_customers = march_data["total_customers"]
    customer_retention_rate = (march_customers / previous_month_customers) * 100
    print(f"Customer Retention Rate: The company retained {customer_retention_rate:.2f}% of its customers from March to march")
else:
    print("No data available for march 2023.")



# COMMAND ----------

# MAGIC %md
# MAGIC Write the Data into Gold layer

# COMMAND ----------

# Write the Data into Gold layer - PART 1
gold_path_claim_policy = f'{mount_point}/outbound/gold/claim_policy'
transformed_df.coalesce(1).write.format('csv').option('header', 'true').mode("overwrite").save(gold_path_claim_policy)

### Rename the file and Delete unwanted file whcih are created
# List the files in the directory to find the CSV file
files = dbutils.fs.ls(gold_path_claim_policy)
# Filter the files to find the one that starts with "part-00000"
part_file = [file for file in files if file.name.startswith("part-00000") and file.name.endswith(".csv")]
# If the part file is found, move and rename it to bronze.csv
if part_file:
    dbutils.fs.mv(part_file[0].path, f"{gold_path_claim_policy}/claim_policy_and_amount.csv")
# Clean up any other files that were created during the write process (e.g., _SUCCESS file)
for file in files:
    if not file.name.endswith(".csv"):
        dbutils.fs.rm(file.path, True)

print("Rename Sucessfully in claim_policy_and_amount")

# COMMAND ----------

# Write the Data into Gold layer - PART 2
gold_path_age_group = f'{mount_point}/outbound/gold/age_group'
age_group_distribution.coalesce(1).write.format('csv').option('header', 'true').mode("overwrite").save(gold_path_age_group)

### Rename the file and Delete unwanted file whcih are created
# List the files in the directory to find the CSV file
files = dbutils.fs.ls(gold_path_age_group)
# Filter the files to find the one that starts with "part-00000"
part_file = [file for file in files if file.name.startswith("part-00000") and file.name.endswith(".csv")]
# If the part file is found, move and rename it to bronze.csv
if part_file:
    dbutils.fs.mv(part_file[0].path, f"{gold_path_age_group}/age_group_distribution.csv")
# Clean up any other files that were created during the write process (e.g., _SUCCESS file)
for file in files:
    if not file.name.endswith(".csv"):

        dbutils.fs.rm(file.path, True)

print("Rename Sucessfully in age_group_distribution")

# COMMAND ----------

# Write the Data into Gold layer - PART 3
gold_path_acquisition_channel = f'{mount_point}/outbound/gold/acquisition_channel'
acquisition_channel_distribution.coalesce(1).write.format('csv').option('header', 'true').mode("overwrite").save(gold_path_acquisition_channel)

### Rename the file and Delete unwanted file whcih are created
# List the files in the directory to find the CSV file
files = dbutils.fs.ls(gold_path_acquisition_channel)
# Filter the files to find the one that starts with "part-00000"
part_file = [file for file in files if file.name.startswith("part-00000") and file.name.endswith(".csv")]
# If the part file is found, move and rename it to bronze.csv
if part_file:
    dbutils.fs.mv(part_file[0].path, f"{gold_path_acquisition_channel}/acquisition_channel_distribution.csv")
# Clean up any other files that were created during the write process (e.g., _SUCCESS file)
for file in files:
    if not file.name.endswith(".csv"):
        dbutils.fs.rm(file.path, True)

print("Rename Sucessfully in acquisition_channel_distribution")

# COMMAND ----------

# Write the Data into Gold layer - PART 4
gold_path_city_distribution = f'{mount_point}/outbound/gold/city_distribution'
city_distribution.coalesce(1).write.format('csv').option('header', 'true').mode("overwrite").save(gold_path_city_distribution)

### Rename the file and Delete unwanted file whcih are created
# List the files in the directory to find the CSV file
files = dbutils.fs.ls(gold_path_city_distribution)
# Filter the files to find the one that starts with "part-00000"
part_file = [file for file in files if file.name.startswith("part-00000") and file.name.endswith(".csv")]
# If the part file is found, move and rename it to bronze.csv
if part_file:
    dbutils.fs.mv(part_file[0].path, f"{gold_path_city_distribution}/city_distribution.csv")
# Clean up any other files that were created during the write process (e.g., _SUCCESS file)
for file in files:
    if not file.name.endswith(".csv"):
        dbutils.fs.rm(file.path, True)

print("Rename Sucessfully in city_distribution")

# COMMAND ----------

# Write the Data into Gold layer - PART 5
# Define the connection details
host = "sancodbserver.mysql.database.azure.com"
port = "3306"
database = "insurance"

# Define MySQL connection properties
mysql_properties = {
    "url": f"jdbc:mysql://{host}:{port}/{database}",
    "user": "nilesh",
    "password": "Sanco@95",    
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Write the DataFrame to MySQL
silverdf.write.format("jdbc").mode("append").option("dbtable", "InsuranceTransactions").options(**mysql_properties).save()
transformed_df.write.format("jdbc").mode("append").option("dbtable", "transformed_df").options(**mysql_properties).save()
age_group_distribution.write.format("jdbc").mode("append").option("dbtable", "age_group_distribution").options(**mysql_properties).save()
acquisition_channel_distribution.write.format("jdbc").mode("append").option("dbtable", "acquisition_channel_distribution").options(**mysql_properties).save()
city_distribution.write.format("jdbc").mode("append").option("dbtable", "city_distribution").options(**mysql_properties).save()

