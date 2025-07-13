# 🚓 UK Police Crime & Stop-and-Search Analytics Project (Azure + Databricks)

This project implements a complete data engineering pipeline on Azure Databricks using real-time data from the [UK Police API](https://data.police.uk/docs/). It follows the **Medallion Architecture** (Bronze → Silver → Gold) and builds rich dashboards that analyze crimes, stop-and-search records, geographic trends, and outcomes.

---

## 🏢 Project Objective

> Build an end-to-end data pipeline to ingest, process, and analyze UK Police data using PySpark and Delta Lake in Azure Databricks. The pipeline provides insights via structured Gold tables and interactive dashboards.

---

## 🔧 Tech Stack

* **Azure Databricks**: Spark-based analytics platform
* **Azure Blob Storage**: Raw and structured data lake storage
* **Delta Lake**: Versioned, ACID-compliant data layer for Silver and Gold
* **PySpark**: Data transformations, aggregations
* **UK Police API**: Live source of crime, search, and officer data
* **Databricks Dashboards**: Visualization with `display()`

---

## 📄 Data Sources

| Data Type        | API Endpoint Example       | Description                    |
| ---------------- | -------------------------- | ------------------------------ |
| Crimes           | `/crimes-street/all-crime` | All crimes by location & date  |
| Stop and Search  | `/stops-street`            | Police search events of people |
| Police Officers  | `/forces/{force}/people`   | List of officers by force      |
| Crime Categories | `/crime-categories`        | Types of crimes with labels    |

---

## 📚 Learning Goals

* Use **structured streaming APIs** for ingestion
* Implement **Medallion Architecture** (Bronze, Silver, Gold)
* Write clean **data validation and flattening logic**
* Join and aggregate across datasets
* Create dashboards directly in **Databricks notebooks**

---

## 📁 Folder Structure (Logical)

```
/raw/               <- Raw JSON from API
  /Crimes/
  /Stop_and_Search/

/silver/            <- Cleaned and flattened datasets
  /Crimes/
  /Stop_and_Search/

/gold/              <- Aggregated datasets for BI/ML
  /Crime_Trend/
  /Top_Streets/
  /Crime_Heatmap/
  /Outcome_Stats/
  /Search_Effectiveness/
```

---

## 🔍 Medallion Architecture Summary

| Layer  | Format | Purpose                                       |
| ------ | ------ | --------------------------------------------- |
| Bronze | JSON   | Raw API response stored in Azure Blob Storage |
| Silver | Delta  | Cleaned and schema-enforced tables            |
| Gold   | Delta  | Aggregated insights, analytics-ready          |

---

## 🔹 Step-by-Step Guide

### ✅ 1. Setup & Configuration

```python
spark.conf.set(
  "fs.azure.account.key.crimedata2203.blob.core.windows.net",
  "<your_storage_key>"
)
```

Set `lat`, `lon`, `date_str`, `data_type` (e.g., "Crimes") in the notebook to define fetch context.

---

### ✅ 2. Ingestion (Bronze Layer)

* API is called using `requests.get()`
* JSON data is saved to Azure Blob under `/raw/<data_type>/<timestamp>`
* Fetch log table tracks each request to prevent duplication

---

### ✅ 3. Cleaning (Silver Layer)

* Apply a defined schema using `StructType`
* Flatten nested fields: `street.name`, `location.latitude`, etc.
* Add columns like `year`, `month_num`, `street_name`, etc.
* Handle nulls and convert types (e.g., string to date)

Stored to `/silver/<data_type>/` as Delta tables.

---

### ✅ 4. Aggregation (Gold Layer)

Generated Gold tables:

#### ▶️ Crime\_Trend

```python
df.groupBy("year", "month_num", "category").agg(count("*").alias("crime_count"))
```

Shows trend of crimes over time by category.

#### ▶️ Top\_Streets

```python
df.groupBy("street_name").agg(count("*").alias("crime_count"))
```

Displays the streets with the highest number of crimes.

#### ▶️ Outcome\_Stats

```python
df.groupBy("category", "outcome_category").agg(count("*").alias("outcome_count"))
```

Summarizes what outcomes were recorded for each crime type.

#### ▶️ Crime\_Heatmap

```python
df.groupBy("latitude", "longitude", "category").agg(count("*").alias("crime_density"))
```

Provides coordinates to build a map-based heatmap.

#### ▶️ Search\_Effectiveness

```python
df_search.join(df_crime, ["street", "year", "month_num"]).groupBy(...).agg(...)
```

Joins stop-and-search and crime data to measure how effective searches are at detecting real crimes.

All gold data is saved using:

```python
df.write.mode("overwrite").partitionBy("year", "month_num").format("delta").save(gold_path)
```

---

## 🔸 Visualization

Use `display(df)` in Databricks notebooks:

* 📅 Line Chart: `crime_trend` → crime count by month/category
* 🔺 Bar Chart: `top_streets`
* 🌍 Map: `crime_heatmap`
* 🔢 Stacked Bar: `outcome_stats`
* 🏛️ Matrix: `search_effectiveness` (search vs actual crime)

---

## 🤯 Common Issues

| Problem             | Fix                                                                                                                                        |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Nulls in Silver     | Check schema, drop bad fields, fill defaults                                                                                               |
| Duplicates in Gold  | Use partitioned write by `year`, `month_num`                                                                                               |
| Overwrites all data | Use `.partitionBy(...).mode("overwrite")`                                                                                                  |
| Widget data loss    | If widgets are used to fetch raw data but not persisted, save output path to log table or variable explicitly to access in other notebooks |

---

## 🚀 Future Scope

* Export Gold data to **Power BI** / **CSV**
* Run **ML models** (e.g., predict arrest likelihood)
* Automate fetch jobs using **Databricks Jobs & Pipelines**

---

## 🌐 Resources

* [UK Police API Docs](https://data.police.uk/docs/)
* [Delta Lake](https://docs.delta.io/latest/index.html)
* [Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/)
* [Databricks Notebooks](https://docs.databricks.com/notebooks/index.html)

---

## 🚀 Author

**Sunny**
Data Engineering Enthusiast | Azure + Spark + Delta Lake
Built as part of a real-time public safety analytics pipeline

---

## ✨ Summary

This beginner-friendly project helps you:

* Connect real-world APIs
* Build layered architecture with Delta Lake
* Learn PySpark data processing
* Visualize structured data in Databricks

> Real data. Real insights. Real engineering.
