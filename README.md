## 📌 Overview

This project demonstrates an end-to-end data engineering pipeline built on Microsoft Azure. The pipeline ingests raw data, processes it using PySpark, and stores it in a scalable data lake architecture.

---

## 🏗️ Architecture

* Azure Data Lake Storage Gen2 (ADLS Gen2)
* Azure Data Factory (ADF)
* Azure Databricks (PySpark)

---

## 🔄 Workflow

1. Data is ingested into the Bronze layer (raw data)
2. Data is cleaned and transformed using Azure Databricks (PySpark)
3. Processed data is stored in the Silver layer
4. Aggregated data is stored in the Gold layer
5. Azure Data Factory orchestrates the pipeline

---

## 📂 Dataset

* AdventureWorks dataset (Sales, Customers, Products, etc.)

---

## ⚙️ Technologies Used

* Python, PySpark
* Azure Data Factory
* Azure Databricks
* ADLS Gen2

---

## 📊 Key Features

* End-to-end ETL pipeline
* Medallion Architecture (Bronze, Silver, Gold)
* Scalable data processing using Spark
* Automated workflows using ADF

---

## 🚀 Future Enhancements

* Add real-time data processing using Event Hub
* Integrate Power BI for visualization
* Implement data quality checks

---

## 👨‍💻 Author

Anand Rathod
