# Databricks notebook source
from datetime import datetime
print(f"🏁 Starting: {dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()}")
print(f"📅 Run time: {datetime.now()}")


# COMMAND ----------

# DBTITLE 1,Run UK Crime Data Pipeline from Raw to Bronze
#%run /Workspace/Users/2100032464@kluniversity.in/UK_Police_Data_Pipeline_AzureDatabricks/Crime_Project/Bronze->Silver


# COMMAND ----------

# DBTITLE 1,Load JSON and Define Silver Path for Delta Table
import json

# Read the JSON file
with open('/tmp/selected_input.json', 'r') as file:
    data = json.load(file)
    
# Define where to store the Delta table (you can use your own path or DBFS)
silver_path = f"dbfs:/mnt/silver/{data['data_type']}"
print(silver_path)


# COMMAND ----------

# DBTITLE 1,Load Crime Data from Silver Delta Table
df_crimes = spark.read.format("delta").load(silver_path)

# COMMAND ----------

# DBTITLE 1,Heatmap Data — Lat/Lon + Category
from pyspark.sql.functions import count

df_crime_heatmap = df_crimes.groupBy(
    "year", "month_num", "latitude", "longitude", "category"
).agg(
    count("*").alias("crime_density")
)

df_crime_heatmap.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("year", "month_num") \
    .option("overwriteSchema", "true") \
    .save("dbfs:/mnt/gold/Crime/crime_heatmap")

display(df_crime_heatmap.select("latitude", "longitude", "category", "crime_density"))

# COMMAND ----------

# DBTITLE 1,Heatmap Data Grouped by Rounded Lat/Lon and Category
from pyspark.sql.functions import round

df_heatmap_clustered = df_crime_heatmap.withColumn("lat_round", round("latitude", 3)) \
                                 .withColumn("lon_round", round("longitude", 3)) \
                                 .groupBy("lat_round", "lon_round", "category") \
                                 .sum("crime_density") \
                                 .withColumnRenamed("sum(crime_density)", "total_crimes")

display(df_heatmap_clustered)


# COMMAND ----------

# DBTITLE 1,Crime Trend by Month and Category
from pyspark.sql.functions import count

df_crime_trend = df_crimes.groupBy("year", "month_num", "category") \
    .agg(count("*").alias("crime_count"))

df_crime_trend.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("year", "month_num") \
    .option("overwriteSchema", "true") \
    .save("dbfs:/mnt/gold/Crime/crime_trend")
display(df_crime_trend)

# COMMAND ----------

# DBTITLE 1,Top Streets by Crime Count
from pyspark.sql.functions import count

df_top_streets = df_crimes.groupBy("year", "month_num", "street_name") \
    .agg(count("*").alias("crime_count")) \
    .orderBy("year", "month_num", "crime_count", ascending=False)

df_top_streets.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("year", "month_num") \
    .option("overwriteSchema", "true") \
    .save("dbfs:/mnt/gold/Crime/top_streets")

df_top_20 = df_top_streets.orderBy("crime_count", ascending=False).limit(20)
display(df_top_20)

# COMMAND ----------

# MAGIC %md
# MAGIC To analyze how often each crime leads to a specific outcome.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Outcome Distribution per Category
df_outcome_stats = df_crimes.groupBy("category", "outcome_category") \
    .agg(count("*").alias("outcome_count")) \
    .orderBy("outcome_count", ascending=False)
    
df_outcome_stats.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .save("dbfs:/mnt/gold/Crime/outcome_stats")
display(df_outcome_stats)