# Databricks notebook source
# MAGIC %md
# MAGIC Mount Nootbook

# COMMAND ----------

# Makr Directory to mount (Optional) 
dbutils.fs.mkdirs('/mnt/adls')

# COMMAND ----------

# Vulnerability Issue Should be Fixed before Push Code to Git hub
access_key1 = 'YBvYJjCR5UHWGsLss+'
access_key2= 'SqOB7kHLUuWGjJzhqhqlR4BOLsCIkayufkbzsx9zbTTtQgia66GM4nny9R+ASt+Q6d0w=='
access_key = access_key1 + access_key2
print(access_key)

# Specify the container and account name
account_name = "insuranceaccount"
container_name = "datacontainer"

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


