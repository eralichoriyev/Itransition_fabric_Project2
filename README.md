# Microsoft Fabric Medallion Project  
**NYC Taxi + OpenAQ Data Pipeline**

## 📌 Project Overview
This project demonstrates a modern data engineering pipeline built in **Microsoft Fabric** using the **Medallion Architecture (Bronze → Silver → Gold)**.

Two datasets are processed:
- 🚕 **NYC Taxi Trip Data**
- 🌍 **OpenAQ Air Quality Data**

The pipeline ingests raw data, cleans and transforms it, and produces analytical tables ready for reporting and visualization.

---

## 🏗️ Architecture

**Bronze Layer**
- Raw NYC Taxi parquet files ingested into Lakehouse
- OpenAQ measurements ingested via API and stored as parquet

**Silver Layer**
- Cleaned NYC Taxi data using Dataflow Gen2
- Cleaned OpenAQ data using Dataflow Gen2
- Removed unnecessary columns and standardized schema

**Gold Layer**
- Aggregated NYC Taxi daily metrics:
  - Average trip distance
  - Average passenger count
  - Average tip amount
  - Total trips per day
- Aggregated OpenAQ metrics:
  - Average pollutant value
  - Min / Max values
  - Measurement count

Gold tables are stored as Delta tables in the Lakehouse.

---

## 🛠️ Technologies Used
- Microsoft Fabric
- Lakehouse
- Dataflow Gen2
- PySpark Notebooks
- Delta Tables
- Power BI Semantic Model

---

## 📂 Repository Structure

