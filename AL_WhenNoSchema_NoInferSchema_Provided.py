# Databricks notebook source
raw_data_location = "dbfs:/tmp/generated_raw_csv_data"
target_delta_table_location = "dbfs:/tmp/table/coordinates"
schema_location = "dbfs:/tmp/auto_loader/schema"
checkpoint_location = "dbfs:/tmp/auto_loader/checkpoint"

# COMMAND ----------

stream =(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true") 
  .option("cloudFiles.schemaLocation", schema_location) 
  .load(raw_data_location))

# COMMAND ----------

display(stream)

# COMMAND ----------

display(stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ### when no schema or inferschema not provided it will treat all column as string so data type change will not fail the stream, but column addition will fail the stream initially
# MAGIC ### but when it restarts again it will capture the additional column but  populate older data value as null in that extra col.

# COMMAND ----------

stream1 =(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")  
  .option("cloudFiles.schemaLocation", schema_location) 
  .load(raw_data_location))

# COMMAND ----------

(stream1.writeStream.option('mergeSchema','true')
 .option("checkpointLocation", checkpoint_location)
 .start(target_delta_table_location))

# COMMAND ----------

(stream1.writeStream
 .option("checkpointLocation", checkpoint_location)
 .start(target_delta_table_location))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/tmp/table/coordinates`

# COMMAND ----------

# MAGIC %md ### Though here used mergeschema it will actually fail if new column added then after restarting new column gets added, but if We dont give option `.option('mergeSchema','true')` it will fail stating that schema mismatch detected in destination location
