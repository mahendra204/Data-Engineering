# Data Engineering Projects Repository

A comprehensive collection of data engineering projects demonstrating modern data pipeline development, Apache Airflow orchestration, PySpark analytics, and cloud data solutions using GCP and Databricks.

## 📚 Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Technologies Used](#technologies-used)
- [Quick Start](#quick-start)
- [Project Descriptions](#project-descriptions)

## 🎯 Overview

This repository contains practical implementations of various data engineering concepts including:
- **Workflow Orchestration** with Apache Airflow
- **Big Data Processing** with Apache Spark
- **Cloud Data Solutions** using Google Cloud Platform (GCP)
- **Declarative Pipelines** with Databricks Delta Live Tables (DLT)
- **ETL/ELT Processes** and Data Transformations
- **Data Quality & Validation** techniques
- **Data Analytics** and Business Intelligence

---

## 📁 Project Structure

### 1. **A Data Engineering Project** 📊
   - **Path:** `A DataEngineeringProject/`
   - **Type:** Jupyter Notebook Project
   - **Description:** Foundational data engineering project with customer and order data analysis
   - **Contents:**
     - Sample CSV datasets (Customers, Orders, OrderItems, Payments, Products)
     - Interactive Jupyter notebook for data exploration and transformation
   - **Key Skills:** Data Loading, EDA, Data Cleaning

### 2. **Airflow** 🔄
   - **Path:** `Airflow/`
   - **Type:** Workflow Orchestration
   - **Description:** Collection of Apache Airflow DAGs demonstrating workflow automation and task scheduling
   - **Key Components:**
     - `first_dag.py` - Introductory DAG with Bash and Python operators
     - `dag_ex1.py` to `dag_ex5.py` - Progressive examples of DAG patterns
     - `decorators_dags.py` - Modern Airflow decorator syntax
     - `decorates_push&pull.py` - XCom data sharing between tasks
     - `python_operators_with_parameters.py` - Parameterized Python operators
     - `docker-compose.yaml` - Docker setup for Airflow environment
   - **Key Skills:** DAG Design, Task Dependencies, Operators, XCom, Scheduling

### 3. **Cloud Storage to BigQuery** ☁️
   - **Path:** `cloudstore_to_bigquery/`
   - **Type:** GCP Data Pipeline
   - **Description:** Implementation of data pipeline from Google Cloud Storage to BigQuery
   - **Contents:**
     - `gcstobigquery_dag.py` - Airflow DAG for GCS → BigQuery ingestion
     - `mock.py` - Mock data generation utilities
     - `local_mockusers_data.csv` - Sample test data
   - **Key Skills:** GCS Integration, BigQuery Loading, Cloud Data Pipelines

### 4. **Data Pipelines** 🛠️
   - **Path:** `DataPipeLines/`
   - **Type:** File-Based ETL
   - **Description:** Basic data transformation pipeline with file operations
   - **Contents:**
     - `script.py` - Core ETL script with filtering, transformation logic
     - `Steel_industryinput.csv` - Sample industrial data
     - Transformations: Date formatting, column filtering, data type conversions
   - **Key Skills:** Data Transformation, File Handling, Data Filtering

### 5. **Datasets** 📦
   - **Path:** `datasets/`
   - **Type:** Sample Data Repository
   - **Description:** Collection of sample CSV files and datasets for testing and learning
   - **Subdirectories:**
     - `Data.csv/` - Organized transaction dataset (2023-2024 with multiple sets)
   - **Datasets:**
     - `Customer_Purchases.csv` - Purchase transaction data
     - `Fashion_Retail_Sales.csv` - Retail sales data
     - `mock_users.csv` - Mock user information
     - `sample_transactions.csv` - Transaction samples
     - `emp_info.txt` - Employee information
     - `depart_info.txt` - Department information
   - **Key Skills:** Data sourcing, Sample data generation

### 6. **Declarative Lakeflow DB Data Pipelines** 🏛️
   - **Path:** `Declarative_lakeflow_DBdatapipelines/`
   - **Type:** Production-Ready Databricks DLT Pipeline
   - **Description:** Enterprise-grade data lakehouse implementation with Databricks Delta Live Tables (DLT) and medallion architecture
   - **Architecture:**
     - **Bronze Layer:** Raw data ingestion and storage
     - **Silver Layer:** Data cleaning, deduplication, and standardization
     - **Gold Layer:** Business-ready analytics and reporting tables
   - **Key Components:**
     - `bronze_layer.py` - Raw data ingestion
     - `silver_layer.py` - Data cleaning and transformations
     - `gold_layer.py` - Analytics-ready datasets
     - `landing_layer.py` - Data landing zone
     - `dlt_pipeline.py` - Main DLT pipeline definition
     - `sample_data_generator.py` - Test data generation
     - `transformations.sql` - SQL transformation logic
     - `utils.py` - Utility functions
     - `requirements.txt` - Python dependencies
     - `PIPELINE_CONFIG.md` - Detailed configuration documentation
     - `metadata.json` - Pipeline metadata
     - `notebooks/` - Setup, data loading, and analytics notebooks
     - `tests/` - Test suite
   - **Key Skills:** Medallion Architecture, Data Quality, Databricks, DLT, Spark SQL

### 7. **Fashion Retail Sales Analysis** 👗
   - **Path:** `Fashion Retail Salesl/`
   - **Type:** PySpark Analytics Project
   - **Description:** Comprehensive ETL and data analysis using PySpark and Spark SQL
   - **Contents:**
     - `Fashion Retail Sales - Data Analysis using PySpark,SparkSQL.ipynb` - Notebook with PySpark analysis
     - Dataset: Fashion retail transactions and customer data
   - **Key Skills:** PySpark, Spark SQL, Data Analysis, Window Functions

### 8. **GCP Projects** ☁️
   - **Path:** `GCP_Projects/`
   - **Type:** Google Cloud Platform Solutions
   - **Description:** Multiple GCP data engineering implementations
   - **Projects:**
     - `pipe1.py` - GCP data pipeline implementation
     - `gcsto_bq.py` - Google Cloud Storage to BigQuery pipeline
     - `employees_info.csv` - Sample employee dataset
   - **Key Skills:** GCP Services, BigQuery, Cloud Storage Integration

### 9. **GCP Pub/Sub** 📬
   - **Path:** `GCP_pubsub/`
   - **Type:** Event Streaming Pipeline
   - **Description:** Real-time data streaming using Google Cloud Pub/Sub
   - **Contents:**
     - `irctc_mock_data_to_pubsub.py` - Mock data producer to Pub/Sub
     - `bigquery_create_table.sql` - BigQuery schema definition
     - `transform_udf.py` - User-defined transformation functions
   - **Key Skills:** Event Streaming, Pub/Sub, Real-time Processing, BigQuery Subscriptions

### 10. **IPL Auction Data ELT** 🏏
   - **Path:** `IPL_auction_data_ELT/`
   - **Type:** Data Analysis Project
   - **Description:** Extract, Load, Transform project analyzing Indian Premier League auction data
   - **Contents:**
     - `IPL_auction_ELT.ipynb` - Interactive Jupyter notebook
     - `IPL_auction_ELT.py` - Python script version
     - `IPL-auction_data.csv` - IPL auction dataset
   - **Key Skills:** Data Analysis, ELT Process, Sports Data Analytics

### 11. **PySpark Scripting Learning** ✨
   - **Path:** `pyspark_Scripting_learn/`
   - **Type:** Educational PySpark Notebooks
   - **Description:** Comprehensive learning materials for PySpark programming
   - **Notebooks:**
     - `pyspark_basic_executions.ipynb` - Fundamental Spark operations
     - `pysaprk.joins, window, aggregates.ipynb` - Advanced transformations
     - `pysaprk_window_explode,split, map, array, selfjoin.ipynb` - Window functions and array operations
     - `PySpark_Revision_Scripting.ipynb` - Comprehensive revision guide
     - `PySpark_Scripting_rev.ipynb` - Additional revision materials
     - `pyspark_scripting2.ipynb` - Extended scripting examples
     - `SCD Implementation.ipynb` - Slowly Changing Dimensions pattern
   - **PySpark/** - Subdirectory with additional scripts
   - **Key Skills:** PySpark Fundamentals, Window Functions, Joins, Complex Transformations, SCD Patterns

---

## 🛠️ Technologies Used

| Technology | Projects | Purpose |
|-----------|----------|---------|
| **Apache Airflow** | Airflow/ | Workflow Orchestration |
| **Apache Spark** | Fashion Retail, PySpark Learning, Declarative Pipelines | Big Data Processing |
| **Databricks DLT** | Declarative Lakeflow DB | Declarative Data Pipelines |
| **Python** | All Projects | Core Development |
| **Jupyter Notebooks** | A Data Engineering Project, IPL Auction, PySpark Learning | Interactive Development |
| **Google Cloud Platform** | GCP Projects, GCP Pub/Sub, Cloud Storage to BigQuery | Cloud Infrastructure |
| **BigQuery** | GCP Projects, Cloud Storage | Data Warehouse |
| **Google Cloud Storage** | Cloud Storage to BigQuery | Data Lake |
| **Google Pub/Sub** | GCP Pub/Sub | Event Streaming |
| **SQL** | Declarative Pipelines, BigQuery | Data Transformation |
| **Docker** | Airflow | Containerization |

---

## 🚀 Quick Start

### Prerequisites
```bash
# Python 3.8+
# Git
# Docker (for Airflow)
```

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd data_engineering
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install basic dependencies:**
   ```bash
   pip install pandas numpy jupyter pyspark
   ```

4. **For Airflow projects:**
   ```bash
   pip install apache-airflow
   # Or use Docker: docker-compose up (in Airflow folder)
   ```

5. **For GCP projects:**
   ```bash
   pip install google-cloud-storage google-cloud-bigquery google-cloud-pubsub
   ```

---

## 📖 Project Descriptions in Detail

### Getting Started Path

**For Beginners:**
1. Start with `A Data Engineering Project/` → Learn data loading and basic transformations
2. Move to `DataPipeLines/` → Understand ETL concepts
3. Explore `datasets/` → Work with sample data

**For Intermediate:**
1. `pyspark_Scripting_learn/` → Master PySpark fundamentals
2. `Airflow/` → Learn workflow orchestration
3. `Fashion Retail Salesl/` → Apply PySpark to real analytics

**For Advanced:**
1. `Declarative_lakeflow_DBdatapipelines/` → Production-grade architectures
2. `GCP_pubsub/` → Real-time streaming solutions
3. `GCP_Projects/` → Cloud-native implementations

---

## 🎓 Learning Outcomes

After working through these projects, you will understand:

✅ Data ingestion and loading strategies  
✅ Data transformation and cleaning patterns  
✅ Workflow orchestration and scheduling  
✅ Big data processing with Apache Spark  
✅ Cloud data solutions with GCP  
✅ Real-time data streaming  
✅ Medallion architecture for data lakehouses  
✅ Data quality and validation  
✅ Production-ready pipeline design  

---

## 📝 Notes

- Each project folder contains detailed documentation in its respective README.md
- Sample datasets are included for testing and learning purposes
- Docker setup available for Airflow to ensure consistency across environments
- All projects follow best practices for error handling and code organization

---

## 📧 Contact & Questions

For questions or improvements, refer to individual project documentation within each folder.

---

**Last Updated:** April 2026  
**Status:** Active Development & Learning
