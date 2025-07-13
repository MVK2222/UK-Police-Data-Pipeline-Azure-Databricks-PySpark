# Databricks notebook source
from datetime import datetime
print(f"🏁 Starting: {dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()}")
print(f"📅 Run time: {datetime.now()}")


# COMMAND ----------

#import json

# Read the JSON file
#with open('/tmp/selected_input.json', 'r') as file:
#    data = json.load(file)
    
# Define where to store the Delta table (you can use your own path or DBFS)
#silver_path = f"dbfs:/mnt/silver/{data['data_type']}"
#print(silver_path)


# COMMAND ----------

crime_path = f"dbfs:/mnt/silver/Crimes"
stopandsearch_path = f"dbfs:/mnt/silver/Stop_and_Search"
gold_crimes_sas_path = f"dbfs:/mnt/gold/multi/crime&stop_search"

# COMMAND ----------

df_crimes = spark.read.format("delta").load(crime_path)
df_stops = spark.read.format("delta").load(stopandsearch_path)

# COMMAND ----------

# DBTITLE 1,Extract Year and Month from Date  in stop&search for better join and parti
from pyspark.sql.functions import year, month

df_extr_yandm = spark.read.format("delta").load(stopandsearch_path)

df_extr_yandm = df_extr_yandm.withColumn("year", year("date")) \
                     .withColumn("month_num", month("date"))

# COMMAND ----------

# DBTITLE 1,Select and Rename Columns for Crimes and Stops Data
from pyspark.sql.functions import col
df_crimes_rename = df_crimes.select( 
        col("street_name").alias("crime_street_name"),
        #col("year").alias("crime_year"),
        #col("month_num").alias("crime_month"),
        "category", "outcome_category", "year", "month_num"
)

df_stops_rename = df_extr_yandm.select(
        col("street_name").alias("stop_street_name"),
        #col("year").alias("stop_year"),
        #col("month_num").alias("stop_month"),
        "object_of_search", "outcome", "gender", "year", "month_num"
)

# COMMAND ----------

# DBTITLE 1,Join Stops and Crimes Data on Street Name and Date
df_joined = df_stops_rename.join(
    df_crimes_rename,
    (df_stops_rename.stop_street_name == df_crimes_rename.crime_street_name) &
    (df_stops_rename.year == df_crimes_rename.year) &
    (df_stops_rename.month_num == df_crimes_rename.month_num),
    how="inner"
)

#df_joined = df_stops_rename.join(
#    df_crimes_rename,
#    (df_stops_rename.stop_street_name == df_crimes_rename.crime_street_name) &
#    (df_stops_rename.stop_year == df_crimes_rename.crime_year) &
#    (df_stops_rename.stop_month == df_crimes_rename.crime_month),
#    how="inner"
#)


# COMMAND ----------

# DBTITLE 1,Group and Count by Search Object and Outcome
from pyspark.sql.functions import count

df_search_effectiveness = df_joined.groupBy(
    #col("stop_year").alias("year"),
    #col("stop_month").alias("month_num"),
    "object_of_search", "category", "outcome", "outcome_category"
).agg(
    count("*").alias("match_count")
).orderBy("match_count", ascending=False)


# COMMAND ----------

display(
    df_search_effectiveness.select(
        "object_of_search", "category", "outcome", "outcome_category", "match_count"
    ).filter("object_of_search = 'Controlled drugs'")
)


# COMMAND ----------

df_search_effectiveness.write.mode("overwrite").format("delta").save(gold_crimes_sas_path)

