# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Loading using AutoLoader
# MAGIC - **Autoloader** --> 
# MAGIC ✅ directory listing 
# MAGIC ✅ file notification 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA Carbon_catalog.net_schema;

# COMMAND ----------

checkpoint_location = "abfss://silver@storagecarbonproject.dfs.core.windows.net/checkpoint" #checkpoint path

# COMMAND ----------

df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation",checkpoint_location)\
  .load("abfss://raw@storagecarbonproject.dfs.core.windows.net/") #cargando datos desde raw (archivo .csv de origen)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# Renombro las columnas problematicas, cuando los indices viene con espacios en blanco, ya que no son aceptados. ✅
df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])

df.writeStream \
    .option("checkpointLocation", checkpoint_location) \
    .trigger(processingTime="10 seconds") \
    .start("abfss://bronze@storagecarbonproject.dfs.core.windows.net/carbon-emissions-borough") # escrito en delta format ✅