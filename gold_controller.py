# Databricks notebook source
# DBTITLE 1,Gold Layer Task Controller for Crime Analysis
# 🎯 Controller Notebook for Gold Layer

dbutils.widgets.dropdown("gold_task", "all", [
    "all",
    "search_outcome",
    "crime_heatmap",
    "search_effectiveness"
])
task = dbutils.widgets.get("gold_task")

print(f"🏁 Running gold task: {task}")

if task in ("all", "search_outcome"):
    print("▶️ Running Gold: Search Outcome Analysis")
    dbutils.notebook.run("/Workspace/Users/2100032464@kluniversity.in/UK_Police_Data_Pipeline_AzureDatabricks/Crime_Project/Single_categ/search_outcome_analysis_(S&S)", 60)

if task in ("all", "crime_heatmap"):
    print("▶️ Running Gold: Crime Heatmap")
    dbutils.notebook.run("/Crime_Project/Single_categ/crime_heatmap_gold", 60)

if task in ("all", "search_effectiveness"):
    print("▶️ Running Gold: Crime + S&S Join")
    dbutils.notebook.run("Crime_Project/Multi_categ/crime&sas", 60)

print("✅ All selected gold tasks completed.")
