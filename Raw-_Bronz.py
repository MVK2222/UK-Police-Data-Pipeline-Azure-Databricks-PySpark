# Databricks notebook source
from datetime import datetime
print(f"🏁 Starting: {dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()}")
print(f"📅 Run time: {datetime.now()}")


# COMMAND ----------

# DBTITLE 1,Extract and Print Widget Values from JSON Config
import json

# Load the saved widget values
with open("/tmp/selected_input.json", "r") as f:
    config = json.load(f)

# Extract values
year = config["year"]
month = config["month"]
city = config["city"]
data_type = config["data_type"]
file_name = config["filename"]

print(f"📅 {year}-{month} | 📍 {city} | 📂 {data_type} | 📄 {file_name}")


# COMMAND ----------

# DBTITLE 1,Blob Storage Config
import json

# Read the JSON file
with open('/tmp/selected_input.json', 'r') as file:
    data = json.load(file)

# This tells Databricks how to access your Azure Storage Account
spark.conf.set(
  "fs.azure.account.key.crimedata.blob.core.windows.net",
  "replace_with_your_storage_account_key"
)

raw_path = f"wasbs://crime-data@crimedata.blob.core.windows.net/raw/{data_type}/{file_name}"

# COMMAND ----------

# DBTITLE 1,Add time stamp column to raw data for filtering
from pyspark.sql.functions import input_file_name, current_timestamp

raw_df = spark.read.json(f"{raw_path}/*.json")

# COMMAND ----------

# DBTITLE 1,Define Schemas for Various Crime Data in PySpark
from pyspark.sql.types import *

# Schema for 'crime'
crime_schema = StructType([
    StructField("category", StringType(), True),
    StructField("context", StringType(), True),
    StructField("id", LongType(), True),
    StructField("location", StructType([
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("street", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True)
        ]))
    ])),
    StructField("location_subtype", StringType(), True),
    StructField("location_type", StringType(), True),
    StructField("month", StringType(), True),
    StructField("outcome_status", StructType([
        StructField("category", StringType(), True),
        StructField("date", StringType(), True)
    ])),
    StructField("persistent_id", StringType(), True)
])

# Schema for 'stop-and-search'
stop_and_search_schema = StructType([
    StructField("age_range", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("involved_person", BooleanType(), True),
    StructField("legislation", StringType(), True),
    StructField("object_of_search", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("operation_name", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("outcome_linked_to_object_of_search", BooleanType(), True),
    StructField("outcome_object", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])),
    StructField("removal_of_more_than_outer_clothing", BooleanType(), True),
    StructField("location", StructType([
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("street", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True)
        ]))
    ])),
    StructField("officer_defined_ethnicity", StringType(), True),
    StructField("self_defined_ethnicity", StringType(), True),
    StructField("type", StringType(), True)
])

# Schema for 'police-force'
force_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("telephone", StringType(), True),
    StructField("engagement_methods", ArrayType(StructType([
        StructField("url", StringType(), True),
        StructField("description", StringType(), True),
        StructField("title", StringType(), True)
    ])))
])

crime_categories_schema = StructType([
    StructField("url", StringType(), True),
    StructField("name", StringType(), True)
])


schema_map = {
    "Crimes": crime_schema,
    "Stop_and_Search": stop_and_search_schema,
    "Police_Officers": force_schema,
    "Crime_Categories": crime_categories_schema
}

# COMMAND ----------

# DBTITLE 1,Read and De-duplicate Raw JSON Data in Spark
import json

# Read the JSON file
with open('/tmp/selected_input.json', 'r') as file:
    data = json.load(file)
    
df_raw = spark.read.schema(schema_map[data_type]).json(f"{raw_path}/*.json")
df_raw.drop_duplicates()

#df_raw.display()

# COMMAND ----------

# DBTITLE 1,Path to store bronze layer data
import json

# Read the JSON file
with open('/tmp/selected_input.json', 'r') as file:
    data = json.load(file)
    
# Define where to store the Delta table (you can use your own path or DBFS)
bronze_path = f"dbfs:/mnt/bronze/{data['data_type']}"  # You can change this as needed


# COMMAND ----------

# DBTITLE 1,Optional ( to delete files in DBFS)
# List files
#dbutils.fs.ls("dbfs:/mnt/bronze")

# Delete folder
#dbutils.fs.rm("dbfs:/mnt/bronze", recurse=True)


# COMMAND ----------

# DBTITLE 1,Write  Bronze data to specified path in delta format using append mode
raw_df.write.format("delta").mode("append").save(bronze_path)


# COMMAND ----------

# DBTITLE 1,Load and Display Bronze Layer Data
df_bronze = spark.read.format("delta").load(bronze_path)
#df_bronze.display()

# COMMAND ----------

# DBTITLE 1,Create Database for Crime Project
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS crime_project;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Bronze Delta Table in Spark
#spark.sql("""
#CREATE TABLE IF NOT EXISTS bronze_{data_type}
#USING DELTA
#LOCATION 'dbfs:/mnt/bronze/{data_type}'
#""")


# COMMAND ----------

#spark.sql("select * from bronze where id= 125748786;").show()