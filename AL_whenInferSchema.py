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
  .option("cloudFiles.inferColumnTypes","true") 
  .option("cloudFiles.schemaLocation", schema_location) 
  .load(raw_data_location))

# COMMAND ----------

display(stream)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### when inferchema is there it will store the data in rescue column for any data type mismatch for existing column but get failed when new column got added and schema got added after rerun the stream read job again

# COMMAND ----------

(stream.writeStream.option('mergeSchema','true')
 .option("checkpointLocation", checkpoint_location)
 .start(target_delta_table_location))


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta.`/tmp/table/coordinates`

# COMMAND ----------

display(stream)

# COMMAND ----------

stream1 =(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true") 
  #.option("rescuedDataColumn", "_rescue") 
  .option("cloudFiles.schemaLocation", schema_location) 
  .load(raw_data_location))

# COMMAND ----------

stream1

# COMMAND ----------

display(stream1)

# COMMAND ----------


