# Databricks notebook source
from datetime import datetime
print(f"🏁 Starting: {dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()}")
print(f"📅 Run time: {datetime.now()}")


# COMMAND ----------

#%pip install azure-storage-blob


# COMMAND ----------

#%restart_python

# COMMAND ----------

# DBTITLE 1,Set Spark Time Zone to Asia/Kolkata
spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")


# COMMAND ----------

# DBTITLE 1,Create Dropdown Widgets for Date and City Selection
from datetime import datetime
import requests
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta

# 🕒 Get current year and month
today = datetime.today()
three_months_back = today - relativedelta(months=3)

current_year = today.year
current_month = three_months_back.month

# 📅 Year dropdown (2022 → current year)
year_options = [str(year) for year in range(2022, current_year + 1)]
dbutils.widgets.dropdown("year", str(current_year), year_options)

# 📆 Month dropdown (1 → 12 or up to current month if current year)
month_options = [f"{m:02d}" for m in range(1, 13)]
if dbutils.widgets.get("year") == str(current_year):
    month_options = [f"{m:02d}" for m in range(1, current_month + 1)]
dbutils.widgets.dropdown("month", f"{current_month:02d}", month_options)

# 📍 City dropdown for latitude and longitude
city_map = {
    "London": ("51.509865", "-0.118092", "metropolitan"),
    "Liverpool": ("53.408371", "-2.991573", "merseyside"),
    "Manchester": ("53.4808", "-2.2426", "greater-manchester"),
    "Bristol": ("51.454514", "-2.587910", "avon-and-somerset"),
    "Leeds": ("53.800755", "-1.549077", "west-yorkshire"),
    "Birmingham": ("52.4862", "-1.8904", "west-midlands")
}
city_options = list(city_map.keys())
dbutils.widgets.dropdown("city", "London", city_options)

# Data type dropdown
dbutils.widgets.dropdown("data_type", "Crimes", [
    "Crimes", "Stop_and_Search", "Police_Officers", "Crime_Categories"
])

# ⛳ Widget inputs
selected_city = dbutils.widgets.get("city")
selected_year = dbutils.widgets.get("year")
selected_month = dbutils.widgets.get("month")
data_type = dbutils.widgets.get("data_type")

lat, lon, force_id = city_map[selected_city]
date_str = f"{selected_year}-{selected_month}"


# COMMAND ----------

# DBTITLE 1,Fetch and Save Police Data to Azure Blob Storage
import json
# ✅ 1. Create table if not exists
spark.sql("""
CREATE TABLE IF NOT EXISTS crime_project.fetch_log (
    year STRING,
    month STRING,
    city STRING,
    data_type STRING,
    file_name STRING,
    timestamp_fetched TIMESTAMP
) USING DELTA 
""")

# ✅ 2. Check if already fetched
log_result = spark.sql(f"""
SELECT file_name FROM crime_project.fetch_log
WHERE year='{selected_year}' AND month='{selected_month}'
AND city='{selected_city}' AND data_type='{data_type}'
ORDER BY timestamp_fetched DESC
LIMIT 1
""").collect()

if log_result:
    file_name = log_result[0]["file_name"]
    print(f"⚠️ This data was already fetched earlier. Skipping fetch. File name saved as: {file_name}")

else:
    # ✅ 3. Proceed with fetching
    print(f"📍 City: {selected_city} | 🗓️ {date_str} | 📊 Type: {data_type}")
    print(f"🌐 Coordinates: lat={lat}, lon={lon} | Force: {force_id}")

    # Build API URL
    if data_type == "Crimes":
        url = f"https://data.police.uk/api/crimes-street/all-crime?lat={lat}&lng={lon}&date={date_str}"
    elif data_type == "Stop_and_Search":
        url = f"https://data.police.uk/api/stops-street?lat={lat}&lng={lon}&date={date_str}"
    elif data_type == "Police_Officers":
        url = f"https://data.police.uk/api/forces/{force_id}/people"
    elif data_type == "Crime_Categories":
        url = f"https://data.police.uk/api/crime-categories?date={date_str}"
    else:
        raise ValueError("❌ Unsupported data type")

    print(f"🔗 Fetching: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to fetch data. Status code: {response.status_code}")
    
    json_data = response.json()
    if len(json_data) == 0:
        print("⚠️ No records found. Skipping save.")
    else:
        print(f"✅ {len(json_data)} records retrieved.")

        # 👇 FIXED: Convert each record to a valid JSON string
        rdd = sc.parallelize([json.dumps(rec) for rec in json_data])
        df_raw = spark.read.json(rdd).withColumn("ingestion_time", current_timestamp())

        # Timestamped file name
        timestamp = datetime.now().strftime("%Y-%m-%d_%I-%M-%S%p")
        file_name = f"{data_type}_{timestamp}"

        # Azure Blob config
        spark.conf.set(
            "fs.azure.account.key.crimedata.blob.core.windows.net",
            "replace_with_your_actual_key_here"
        )

        # Save path
        raw_path = f"wasbs://crime-data@crimedata2203.blob.core.windows.net/raw/{data_type}/{file_name}"
        df_raw.write.mode("overwrite").format("json").save(raw_path)
        print("✅ Data written to Azure Blob Storage.")

        # ✅ 4. Log this fetch in fetch_log table
        log_df = spark.createDataFrame([{
            "year": selected_year,
            "month": selected_month,
            "city": selected_city,
            "data_type": data_type,
            "file_name": file_name,
            "timestamp_fetched": datetime.now()
        }])
        log_df.write.mode("append").format("delta").saveAsTable("crime_project.fetch_log")


# COMMAND ----------

# DBTITLE 1,Save Selected Widget Values to Local JSON File
import json
from datetime import datetime

# Collect selected values
selected_info = {
    "year": dbutils.widgets.get("year"),
    "month": dbutils.widgets.get("month"),
    "city": dbutils.widgets.get("city"),
    "data_type": dbutils.widgets.get("data_type"),
    "timestamp": datetime.now().isoformat(),
    "filename": file_name
}

# Save locally in Databricks (overwrites every time)
with open("/tmp/selected_input.json", "w") as f:
    json.dump(selected_info, f)

print("✅ Selection saved locally in /tmp/selected_input.json")


# COMMAND ----------

# DBTITLE 1,Clear All Existing Widgets
dbutils.widgets.removeAll()
