# Databricks notebook source
access_key1 = 'Szm6BbKUvSaUBED/'
access_key2= 'f6NOFxHHsLFQKrg5TLjGqaaGqI8I6GtzfoC1H+N9plmKdg9eXGJD5aV9c2Hq+AStcVhr8g=='
access_key = access_key1 + access_key2

# Specify the container and account name
container_name = "continer1"
account_name = "insuranceaccount1"

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

# Display the content of the DataFrame
df.show()


