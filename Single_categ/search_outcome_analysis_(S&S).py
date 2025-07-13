# Databricks notebook source
# DBTITLE 1,Notebook Run Information
from datetime import datetime
print(f"🏁 Starting: {dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()}")
print(f"📅 Run time: {datetime.now()}")


# COMMAND ----------

# DBTITLE 1,Generate Delta Table Path from JSON
import json

# Read the JSON file
with open('/tmp/selected_input.json', 'r') as file:
    data = json.load(file)
    
# Define where to store the Delta table (you can use your own path or DBFS)
silver_path = f"dbfs:/mnt/silver/{data['data_type']}"
print(silver_path)


# COMMAND ----------

# DBTITLE 1,Load Silver DataFrame from Delta Format
df_silver = spark.read.format("delta").load(silver_path)

# COMMAND ----------

# DBTITLE 1,Aggregate Search Data by Gender and Ethnicity
from pyspark.sql.functions import count

df_agg = df_silver.groupBy(
    "gender",
    "officer_ethnicity",
    "self_ethnicity",
    "object_of_search",
    "outcome"
).agg(
    count("*").alias("total_searches")
)

# COMMAND ----------

# DBTITLE 1,Calculate Percentage of Searches by Object Type
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum, round, col

# Window per object_of_search group
w = Window.partitionBy("object_of_search")

df_percent = df_agg.withColumn(
    "percent", round(col("total_searches") * 100 / _sum("total_searches").over(w), 2)
)


# COMMAND ----------

# DBTITLE 1,Rename Outcome Column with Friendly Descriptions
from pyspark.sql.functions import when

df_renamed = df_percent.withColumn(
    "outcome_friendly",
    when(col("outcome") == "A no further action disposal", "No Action Taken")
    .when(col("outcome") == "Community resolution", "Resolved Informally")
    .when(col("outcome") == "Arrest", "Arrested")
    .when(col("outcome") == "Summons / charged by post", "Charged by Post")
    .when(col("outcome") == "Caution (simple or conditional)", "Given Caution")
    .when(col("outcome") == "Khat or Cannabis warning", "Drug Warning")
    .when(col("outcome") == "Not specified", "Unknown Outcome")
    .when(col("outcome") == "Evidence of offences under the Act", "Evidence Found")
    .otherwise("Other")
)


# COMMAND ----------

# DBTITLE 1,Read and Process JSON to Generate Gold Path
import json

# Read the JSON file
with open('/tmp/selected_input.json', 'r') as file:
    data = json.load(file)

gold_path = f"dbfs:/mnt/gold/{data['data_type']}"

# COMMAND ----------

# DBTITLE 1,Save and Register Gold Search Outcome Analysis Table
df_gold = df_renamed
df_gold.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(gold_path)

# Optional: Register as table
spark.sql("DROP TABLE IF EXISTS gold_search_outcome_analysis")
spark.sql(f"""
CREATE TABLE gold_search_outcome_analysis
USING DELTA
LOCATION '{gold_path}'
""")


# COMMAND ----------

# DBTITLE 1,Aggregate Search Outcomes by Gender and Results
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   gender, 
# MAGIC   outcome_friendly, 
# MAGIC   SUM(total_searches) AS total
# MAGIC FROM gold_search_outcome_analysis
# MAGIC GROUP BY gender, outcome_friendly
# MAGIC ORDER BY total DESC
# MAGIC