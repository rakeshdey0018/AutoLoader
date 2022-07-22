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
  .schema("id string,x_axis int,y_axis int") 
  .load(raw_data_location))

# COMMAND ----------

display(stream)

# COMMAND ----------

stream1 =(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")
  .schema("id string,x_axis int,y_axis int") 
  .option("rescuedDataColumn", "_rescue") 
  .load(raw_data_location))

# COMMAND ----------

display(stream1)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### when Schema provided for any data type mismatch or any new column addition it will not fail ignore the records.. But if we give `.option("rescuedDataColumn", "_rescue") ` then add malformatted record to rescue column
