# Databricks notebook source
from datetime import datetime
print(f"🏁 Starting: {dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()}")
print(f"📅 Run time: {datetime.now()}")


# COMMAND ----------

# DBTITLE 1,Run Crime Data Pipeline from Raw to Bronze
# MAGIC %run /Workspace/Users/2100032464@kluniversity.in/UK_Police_Data_Pipeline_AzureDatabricks/Crime_Project/Raw->Bronz
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Bronze Data from Delta Table
df_bronze = spark.read.format("delta").load(bronze_path)
#df_bronze.display()

# COMMAND ----------

# DBTITLE 1,Enhance Crime Datasets with Transformation Functions
from pyspark.sql.functions import col, upper, when, to_date, year, month, explode, trim

def transform_data(df, data_type):
    if data_type == "Crimes":
        print("🔁 Applying transformation for Crimes dataset...")
        # Transform the Crimes data
        return df \
            .withColumn("latitude", col("location.latitude").cast("double")) \
            .withColumn("longitude", col("location.longitude").cast("double")) \
            .withColumn("street_name", col("location.street.name")) \
            .withColumn("outcome_category", col("outcome_status.category")) \
            .withColumn("outcome_date", col("outcome_status.date")) \
            .withColumn("year", year("month")) \
            .withColumn("month_num", month("month")) \
            .withColumn("persistent_id",when(col("persistent_id").isNull() | (trim(col("persistent_id")) == ""),"MISSING").otherwise(col("persistent_id"))) \
            .withColumn("location_subtype", when(trim(col("location_subtype")) == "", "Unknown").otherwise(col("location_subtype")))

    elif data_type == "Stop_and_Search":
        # Transform the Stop and Search data
        print("🔁 Applying transformation for Stop and Search dataset...")
        return df \
            .withColumn("latitude", col("location.latitude").cast("double")) \
            .withColumn("longitude", col("location.longitude").cast("double")) \
            .withColumn("street_name", col("location.street.name")) \
            .withColumn("date", to_date("datetime")) \
            .withColumn("gender", upper(col("gender"))) \
            .withColumn("officer_ethnicity", upper(col("officer_defined_ethnicity"))) \
            .withColumn("self_ethnicity", upper(col("self_defined_ethnicity"))) \
            .withColumn("outcome_id", col("outcome_object.id")) \
            .withColumn("outcome_name", col("outcome_object.name")) \
            .withColumnRenamed("type", "search_type") \
            .withColumn("outer_clothing_removed", when(col("removal_of_more_than_outer_clothing"), 1) .otherwise(0))

    elif data_type == "Police_Officers":
        # Transform the Police Officers data
        print("🔁 Applying transformation for Police Officers dataset...")
        return df \
            .withColumn("engagement", explode(col("engagement_methods"))) \
            .withColumn("engagement_url", col("engagement.url")) \
            .withColumn("engagement_title", col("engagement.title")) \
            .withColumn("engagement_desc", col("engagement.description")) \
            .drop("engagement_methods")

    elif data_type == "Crime_Categories":
        # Remove duplicate Crime Categories
        print("🔁 Applying transformation for Crime Categories dataset...")
        return df.dropDuplicates(["name"])

    else:
        # Raise an error for unsupported data types
        raise ValueError("❌ Unsupported data type: " + data_type)

df_transformed = transform_data(df_bronze, data_type)
print("✅ Transformation completed for " + data_type + " dataset")

# COMMAND ----------

# DBTITLE 1,Cleaning and Null Handling for Various Data Types
from pyspark.sql.functions import col, when, lit

def clean_data(df, data_type):
    if data_type == "Crimes":
        print("🧹 Cleaning and null handling for Crimes data...")

        # Drop flattened structs
        df = df.drop("location", "outcome_status","month","context")

        # Fill nulls
        df = df.fillna({
            "category": "Unknown",
            #"context": "None",
            "location_type": "Unknown",
            "location_subtype": "Unknown",
            "persistent_id": "Unavailable",
            "outcome_category": "Not recorded",
            "outcome_date": "Not available",
            "street_name": "Unnamed street"
        })

    elif data_type == "Stop_and_Search":
        print("🧹 Cleaning and null handling for Stop and Search data...")

        # Drop flattened structs
        df = df.drop("location", "outcome_object","datetime",
            "operation",
            "officer_defined_ethnicity",
            "self_defined_ethnicity"
            )

        # Fill nulls
        df = df.fillna({
            "age_range": "Unknown",
            "gender": "Not specified",
            "legislation": "Unknown",
            "object_of_search": "Not specified",
            #"operation": "None",
            #"operation_name": "Unknown",
            "outcome": "Not recorded",
            #"officer_defined_ethnicity": "Unknown",
            "officer_ethnicity": "Not specified",
            "self_ethnicity": "Not specified",
            "search_type": "Unknown",
            "street_name": "Unnamed street",
            "outcome_name": "Not recorded",
            "outcome_linked_to_object_of_search": "False"
        })

    elif data_type == "Police_Officers":
        print("🧹 Cleaning and null handling for Police Officers data...")

        # Drop flattened array
        df = df.drop("engagement_methods")

        # Fill nulls
        df = df.fillna({
            "name": "Unknown",
            "description": "Not available",
            "url": "N/A",
            "telephone": "N/A",
            "engagement_url": "N/A",
            "engagement_title": "Unknown",
            "engagement_desc": "Not provided"
        })

    elif data_type == "Crime_Categories":
        print("🧹 Cleaning and null handling for Crime Categories data...")

        # Fill nulls
        df = df.fillna({
            "url": "N/A",
            "name": "Unknown Category"
        })

    else:
        raise ValueError(f"❌ Unsupported data type: {data_type}")

    return df
df_cleaned = clean_data(df_transformed, data_type)
print("✅ Cleaning and null handling completed for " + data_type + " dataset")

# COMMAND ----------

# DBTITLE 1,Read JSON and Define Silver Table Path in DBFS
import json

# Read the JSON file
with open('/tmp/selected_input.json', 'r') as file:
    data = json.load(file)
    
# Define where to store the Delta table (you can use your own path or DBFS)
silver_path = f"dbfs:/mnt/silver/{data['data_type']}"
print("✅ Silver table path: " + silver_path)


# COMMAND ----------

# DBTITLE 1,Delete Files from DBFS Directories
# List files
#dbutils.fs.ls("dbfs:/mnt/bronze")

# Delete folder
#dbutils.fs.rm("dbfs:/mnt/silver/Crimes", recurse=True)


# COMMAND ----------

# DBTITLE 1,Update Silver Layer with Deduplicated Data
from pyspark.sql.utils import AnalysisException

# Define deduplication keys per data_type
dedup_keys_map = {
    "Crimes": ["persistent_id"],
    "Stop_and_Search": ["datetime", "gender", "object_of_search", "street_name"],
    "Police_Officers": ["id"],
    "Crime_Categories": ["url"]
}

try:
    # Try to load existing Silver table
    df_existing = spark.read.format("delta").load(silver_path)
    
    # Union new with existing and drop duplicates based on keys
    df_silver = df_existing.union(df_cleaned).dropDuplicates(dedup_keys_map[data_type])
    
except AnalysisException:
    # If table doesn't exist yet, just use new cleaned data
    df_silver = df_cleaned

# Write merged data back to Silver
df_silver.write.mode("overwrite").format("delta").save(silver_path)

print(f"✅ Silver table updated for data_type = '{data_type}' with deduplication on {dedup_keys_map[data_type]}")


# COMMAND ----------

# DBTITLE 1,Save Deduplicated Data to Silver Delta Table
#df_silver =df_cleaned.dropDuplicates()
#df_silver.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------

# DBTITLE 1,Count and Display Null or Blank Values in Columns
#from pyspark.sql.functions import count, when, trim

#df_silver.select([
#    count(when(col(c).isNull() | (trim(col(c)) == ""), c)).alias(c + "_null_or_blank")
#    for c in df_silver.columns
#]).display()


# COMMAND ----------

# DBTITLE 1,Display Silver Layer DataFrame
#df_silver.display()