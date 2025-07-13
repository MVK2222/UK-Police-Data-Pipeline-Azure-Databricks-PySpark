# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.crimedata2203.blob.core.windows.net",
  "IKzZVm6WeTJv7mKn/UK4JN/bhY2ZaO/4qbtOKMcVpmn7pDjO1aS+wXwxSeYrJ9xIOkuRN8Jq1YRt+AStiXiZmQ=="
)


# COMMAND ----------

import requests
url = "https://data.police.uk/api/crimes-street/all-crime?lat=51.509865&lng=-0.118092&date=2024-12"
response = requests.get(url)
print(response.status_code)
print(response.text[:300])  # Show first few characters to check


# COMMAND ----------

import requests

# Build the API URL
crime_url = f"https://data.police.uk/api/crimes-street/all-crime?lat={lat}&lng={lon}&date={date_str}"

# Fetch the data
response = requests.get(crime_url)

# Check if response is valid JSON and status is 200
if response.status_code == 200:
    try:
        crime_data = response.json()
        if crime_data:
            print(f"✅ Retrieved {len(crime_data)} records")
        else:
            print("⚠️ No data returned for the selected inputs.")
    except Exception as e:
        print("❌ JSON decode failed. Response was not valid JSON.")
        print(response.text)
else:
    print(f"❌ Failed to fetch data. Status code: {response.status_code}")
    print(response.text)
