# Databricks notebook source
# Import necessary libraries
import pandas as pd 
from pyspark.sql import SparkSession 

# COMMAND ----------

# Create a Spark session
spark = SparkSession.builder.appName('ArrowExample').getOrCreate()

# COMMAND ----------

# Generate a sample Spark DataFrame
data = [("alice",1),("bob",2),("charlie",3)]
columns=["Name","Value"]
df_Spark = spark.createDataFrame(data,columns)

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame using Arrow
pandas_df_arrow = df_Spark.select("*").toPandas()

# COMMAND ----------

# Adding an index to the Arrow Pandas DataFrame
pandas_df_arrow.set_index("Name",inplace=True)

# COMMAND ----------

# Perform Pandas operations on the local Pandas DataFrame obtained from Arrow
pandas_df_arrow["SquaredValue"] = pandas_df_arrow["Value"] ** 2

# COMMAND ----------

# Convert the modified Pandas DataFrame back to a Spark DataFrame
df_spark_modified = spark.createDataFrame(pandas_df_arrow)

# COMMAND ----------

# Show the modified Spark DataFrame
df_spark_modified.show()

# COMMAND ----------


