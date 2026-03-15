# End-to-End Data Engineering: Declarative Data Pipelines

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.8%2B-green.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/databricks-compatible-green.svg)](https://databricks.com/)

A comprehensive, production-ready data pipeline implementation using **Databricks Delta Live Tables (DLT)** and **Apache Spark** with the medallion architecture (Bronze → Silver → Gold) for enterprise e-commerce data processing.

## 📋 Project Overview

This project demonstrates a complete end-to-end data engineering solution with:

✅ **Declarative Data Pipelines** using Databricks DLT  
✅ **Medallion Architecture** (Bronze/Silver/Gold layers)  
✅ **Data Quality Expectations** with built-in validation  
✅ **Spark Lakeflow Implementation** for scalable processing  
✅ **Comprehensive Data Models** for e-commerce scenarios  
✅ **Production-Ready Code** with error handling & monitoring  
✅ **Complete Sample Datasets** with realistic data  
✅ **Analytics Notebooks** for business insights  

## 🏗️ Architecture Overview

### Medallion Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Analytics)                    │
│  ├─ gold_customer_orders      ├─ gold_product_performance    │
│  ├─ gold_monthly_revenue      ├─ gold_category_summary       │
│  ├─ gold_top_customers        ├─ gold_enriched_orders        │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleaned)                     │
│  ├─ silver_customers          ├─ silver_products             │
│  ├─ silver_orders (validated, deduplicated)                  │
│  Quality Checks: Expectations applied                        │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw)                        │
│  ├─ bronze_customers          ├─ bronze_products             │
│  ├─ bronze_orders (no transforms)                            │
│  CSV files ingested as-is                                    │
└─────────────────────────────────────────────────────────────┘
```

### Directory Structure

```
declarative_datapipelines/
├── data/                          # Sample e-commerce data
│   ├── customers.csv             # 100 customer records
│   ├── products.csv              # 50 product records
│   └── orders.csv                # 300 order transactions
│
├── src/                          # Core source code
│   ├── dlt_pipeline.py          # 🔥 Main DLT pipeline (500+ lines)
│   ├── spark_lakeflow_pipeline.py # Spark Lakeflow impl (550+ lines)
│   ├── sample_data_generator.py  # Realistic data generator
│   ├── utils.py                 # Utilities & helpers
│   ├── config.py                # Configuration management
│   └── transformations.sql      # SQL transformations
│
├── notebooks/                     # Databricks notebooks
│   ├── 01_Setup.py              # Environment initialization
│   ├── 02_LoadSampleData.py     # Data loading & ingestion
│   └── 03_Analytics.py          # Analytics & insights
│
├── tests/                        # Unit tests
│   └── test_pipeline.py
│
├── requirements.txt              # Python dependencies
├── README.md                    # This file
├── .gitignore                   # Git configuration
├── PIPELINE_CONFIG.md           # Detailed config guide
├── metadata.json                # Project metadata
└── LICENSE                      # MIT License
```

## 🗂️ Data Layers Details

### Bronze Layer (Raw Data Ingestion)
- **Purpose**: Store raw data as-is from source systems
- **Transformations**: None
- **Tables**:
  - `bronze_customers` - Raw customer data
  - `bronze_products` - Raw product catalog
  - `bronze_orders` - Raw order transactions

### Silver Layer (Data Cleaning & Validation)
- **Purpose**: Clean, deduplicate, and validate data
- **Transformations**:
  - Type conversions (cast to appropriate types)
  - Null handling and deduplication
  - Data quality expectations applied
- **Quality Expectations**:
  - Email format validation
  - Price > 0 validation
  - Non-null customer/product IDs
- **Tables**:
  - `silver_customers` - Cleaned customer data
  - `silver_products` - Cleaned product data
  - `silver_orders` - Cleaned order data

### Gold Layer (Analytics & Business Logic)
- **Purpose**: Aggregated, business-ready datasets
- **Transformations**: Complex aggregations, joins, window functions
- **Key Tables**:
  - `gold_customer_orders` - Customer purchase summaries
  - `gold_product_performance` - Product sales metrics
  - `gold_monthly_customer_revenue` - Monthly trends
  - `gold_category_summary` - Category analytics
  - `gold_top_customers` - VIP customer ranking
  - `gold_enriched_orders` - Complete order details with context

## 📊 Sample Data

The project includes realistic e-commerce sample data:

- **100 Customers** across 15 US cities and 10 countries
- **50 Products** across 9 product categories
- **300 Orders** spanning 6-month period with order statuses

Sample data is generated automatically via `sample_data_generator.py`

## 🚀 Getting Started

### Prerequisites

```bash
- Python 3.8 or higher
- Databricks workspace (Community or Enterprise)
- Git for version control
- Apache Spark 3.5+
```

### Quick Start

#### 1. Clone Repository

```bash
git clone https://github.com/mahendra204/Data-Engineering.git
cd declarative_datapipelines
```

#### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 3. Generate Sample Data

```bash
python src/sample_data_generator.py
```

Generate sample data will create CSV files in `data/` directory.

#### 4. Deploy to Databricks

**Option A: Using Databricks CLI**

```bash
# Create workspace folders
databricks workspace mkdirs /Shared/declarative_pipelines

# Upload source files
databricks workspace import src/ /Shared/declarative_pipelines/src --language PYTHON

# Upload notebooks
databricks workspace import notebooks/ /Shared/declarative_pipelines/notebooks --language PYTHON

# Upload data
databricks dbfs cp data/ dbfs:/data --recursive
```

**Option B: Using Databricks UI**

1. Create a cluster in your Databricks workspace
2. Create notebooks from files in `notebooks/` folder
3. Upload CSV files to `/dbfs/data/`
4. Run notebooks in order:
   - 01_Setup
   - 02_LoadSampleData
   - 03_Analytics

### Creating DLT Pipeline

1. In Databricks UI, go to **Workflows** → **Delta Live Tables**
2. Create new pipeline:
   - **Name**: `ecommerce_declarative_pipeline`
   - **Notebook**: Point to `src/dlt_pipeline.py`
   - **Target Catalog**: `main`
   - **Target Schema**: `pipeline_tables`
3. Click **Start** to run pipeline

## 💻 Code Examples

### Using DLT Pipeline Directly

```python
from src.dlt_pipeline import bronze_customers, silver_customers, gold_customer_orders

# In Databricks notebook:
# The DLT decorators handle everything automatically
# Just reference the tables:

customer_insights = spark.sql("""
    SELECT 
        customer_id, 
        first_name, 
        total_orders, 
        total_spent
    FROM main.gold_ecommerce.gold_customer_orders
    WHERE total_orders > 5
    ORDER BY total_spent DESC
""")
```

### Using Spark Lakeflow Pipeline

```python
from src.spark_lakeflow_pipeline import SparkLakeflowPipeline

# Initialize pipeline
pipeline = SparkLakeflowPipeline(
    catalog="main",
    bronze_schema="bronze_ecommerce",
    silver_schema="silver_ecommerce",
    gold_schema="gold_ecommerce"
)

# Run full pipeline
pipeline.run_full_pipeline()

# Display statistics
pipeline.show_pipeline_stats()
```

### Generating Sample Data

```python
from src.sample_data_generator import SampleDataGenerator

generator = SampleDataGenerator(
    num_customers=1000,
    num_products=500,
    num_orders=5000,
    seed=42
)

data = generator.generate_all(output_dir="data")
```

## 📈 Key Metrics & Analytics

The pipeline generates the following analytics:

### Customer Analytics
- Total number of customers
- Customer lifetime value (total spent)
- Average order value per customer
- Order frequency analysis
- Top 100 customers by spending

### Product Analytics
- Total units sold per product
- Revenue by product
- Average order value per product
- Category-wise performance
- Top products by revenue

### Sales Analytics
- Monthly revenue trends
- Daily sales aggregation
- Order status distribution
- Category-wise sales summary
- Time-series analysis

## 🧪 Testing

Run the test suite:

```bash
pytest tests/ -v --cov=src
```

## 📝 Configuration

Edit `src/config.py` for custom configuration:

```python
# Catalog & Schema names
CATALOG_NAME = "main"
SCHEMA_BRONZE = "bronze_ecommerce"
SCHEMA_SILVER = "silver_ecommerce"
SCHEMA_GOLD = "gold_ecommerce"

# Data paths
SOURCE_DATA_PATH = "/dbfs/data"

# Refresh settings
REFRESH_INTERVAL = "1 hour"
```

## 🔍 Monitoring & Troubleshooting

### View DLT Pipeline Details

```sql
-- Check table row counts
SELECT table_name, record_count FROM main.gold_ecommerce.tables_info

-- View data quality metrics
SELECT * FROM main.gold_ecommerce.dlt_expectations_audit
```

### Common Issues

| Issue | Solution |
|-------|----------|
| File not found at `/dbfs/data` | Upload CSV files or run `sample_data_generator.py` |
| Schema not exists | Run setup notebook 01_Setup first |
| Null pointer exception | Check data quality - some records may be invalid |

## 📚 Documentation

- [Pipeline Configuration Guide](PIPELINE_CONFIG.md)
- [Data Model & Schema Documentation](DOC_SCHEMAS.md)
- [DLT Best Practices](https://docs.databricks.com/data-engineering/delta-live-tables/)

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 👨‍💼 Author

**Mahendra** - Data Engineering Portfolio  
GitHub: [@mahendra204](https://github.com/mahendra204)

## 🙏 Acknowledgments

- Databricks for Delta Live Tables framework
- Apache Spark community
- E-commerce data modeling best practices

## 📞 Support

For issues, questions, or suggestions:
1. Open an [GitHub Issue](https://github.com/mahendra204/Data-Engineering/issues)
2. Check existing documentation
3. Review [PIPELINE_CONFIG.md](PIPELINE_CONFIG.md)

## 🎯 Roadmap

- [ ] Add streaming data ingestion support
- [ ] Implement ML feature engineering pipelines
- [ ] Add real-time monitoring dashboard
- [ ] Support for multiple data sources (JSON, Parquet)
- [ ] Advanced data quality framework
- [ ] Cost optimization utilities

---

**Last Updated**: March 15, 2026  
**Version**: 2.0.0  
**Status**: Production Ready ✅


## 📋 Overview

This project demonstrates an enterprise-grade data pipeline architecture using declarative programming patterns with Delta Live Tables. It follows the medallion architecture pattern (Bronze → Silver → Gold) for data transformation and quality management.

### Key Features

- **Declarative Pipeline Definition**: Uses DLT for declarative data transformations
- **Multi-Layer Architecture**: Bronze (Raw), Silver (Cleaned), Gold (Analytics)
- **Data Quality Checks**: Built-in data quality expectations and validations
- **Sample E-Commerce Data**: Complete sample dataset with customers, products, and orders
- **Scalable Design**: Optimized for Databricks environments
- **Production Ready**: Includes error handling, monitoring, and best practices

## 🏗️ Architecture

### Directory Structure

```
declarative_datapipelines/
├── data/                      # Sample data files
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
├── src/                       # Source code
│   ├── dlt_pipeline.py       # Main DLT pipeline definitions
│   ├── config.py             # Pipeline configuration
│   └── transformations.sql   # SQL transformation definitions
├── notebooks/                # Databricks notebooks
│   ├── 01_Setup.py          # Setup and initialization
│   ├── 02_LoadSampleData.py # Load sample data
│   └── 03_Analytics.py      # Analytics queries
├── tests/                    # Unit tests
│   └── test_pipeline.py
├── requirements.txt          # Python dependencies
├── README.md                # Documentation
├── .gitignore               # Git ignore rules
└── PIPELINE_CONFIG.md       # Pipeline configuration guide
```

## 📊 Data Layers

### Bronze Layer
- Raw data ingestion from source systems
- No transformations, minimal validation
- Tables: `bronze_customers`, `bronze_products`, `bronze_orders`

### Silver Layer
- Cleaned and validated data
- Type conversions, null handling
- Data quality expectations applied
- Tables: `silver_customers`, `silver_products`, `silver_orders`

### Gold Layer
- Business-ready analytics tables
- Aggregations and KPIs calculated
- Tables:
  - `gold_customer_orders`: Customer purchasing behavior
  - `gold_product_sales`: Product performance metrics
  - `gold_order_metrics`: Order status distributions
  - `gold_daily_sales`: Daily sales dashboards
  - `gold_category_performance`: Category-wise analytics

## 🚀 Getting Started

### Prerequisites

- Databricks workspace (Community or Enterprise)
- Python 3.8+
- Apache Spark 3.5+
- Git

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/mahendra204/Data-Engineering.git
cd declarative_datapipelines
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Set up Databricks CLI** (if needed)
```bash
databricks configure --host https://your-databricks-instance.com --token <your-token>
```

### Deployment to Databricks

#### Option 1: Using Databricks UI
1. Create a new cluster in your Databricks workspace
2. Create a new notebook and copy the contents from `notebooks/01_Setup.py`
3. Run the setup notebook
4. Run `notebooks/02_LoadSampleData.py` to load sample data
5. Create a DLT pipeline from `src/dlt_pipeline.py`

#### Option 2: Using Databricks CLI
```bash
# Create DLT pipeline
databricks pipelines create --config dlt_pipeline_config.json

# Deploy code to workspace
databricks workspace import_directory ./notebooks /Users/me/dlt-notebooks --overwrite

# Run pipeline
databricks pipelines start --pipeline-id <pipeline-id>
```

#### Option 3: Manual Setup in Databricks

1. **Create schemas**:
```sql
CREATE SCHEMA bronze_ecommerce;
CREATE SCHEMA silver_ecommerce;
CREATE SCHEMA gold_ecommerce;
```

2. **Load sample data**:
```sql
CREATE TABLE bronze_ecommerce.bronze_customers
USING csv
LOCATION '/path/to/data/customers.csv'
OPTIONS (header 'true', inferSchema 'true');
```

3. **Create DLT pipeline**:
- Create a new DLT pipeline in Databricks
- Point it to `src/dlt_pipeline.py`
- Configure cluster settings
- Start the pipeline

## 📈 Sample Data Schema

### Customers Table
| Column | Type | Description |
|--------|------|-------------|
| customer_id | String | Unique customer identifier |
| first_name | String | Customer first name |
| last_name | String | Customer last name |
| email | String | Customer email address |
| phone | String | Contact phone number |
| city | String | City of residence |
| country | String | Country of residence |
| registration_date | Date | Account registration date |

### Products Table
| Column | Type | Description |
|--------|------|-------------|
| product_id | String | Unique product identifier |
| product_name | String | Product name |
| category | String | Product category |
| price | Decimal | Product price |
| stock_quantity | Integer | Current stock level |
| supplier_id | String | Supplier identifier |
| created_date | Date | Product creation date |

### Orders Table
| Column | Type | Description |
|--------|------|-------------|
| order_id | String | Unique order identifier |
| customer_id | String | Customer reference |
| product_id | String | Product reference |
| quantity | Integer | Order quantity |
| order_date | Date | Order placement date |
| delivery_date | Date | Delivery date (nullable) |
| order_status | String | Current order status |
| total_amount | Decimal | Order total amount |

## 🔍 Data Quality Checks

The pipeline includes comprehensive data quality expectations:

**Customers:**
- Valid email format check
- Non-null phone validation
- Unique customer ID validation

**Products:**
- Price > 0 validation
- Stock quantity >= 0 validation
- Unique product ID validation

**Orders:**
- Quantity > 0 validation
- Non-null customer_id validation
- Order total amount validation

## 📝 Running Analytics

### SQL Queries

Run these queries on the Gold layer for insights:

```sql
-- Top customers by spending
SELECT customer_id, total_spent, total_orders
FROM gold_ecommerce.gold_customer_orders
ORDER BY total_spent DESC
LIMIT 10;

-- Product performance
SELECT product_name, units_sold, total_revenue, avg_order_value
FROM gold_ecommerce.gold_product_sales
ORDER BY total_revenue DESC;

-- Order distribution
SELECT order_status, order_count, revenue, percentage
FROM gold_ecommerce.gold_order_metrics;

-- Daily trends
SELECT order_date, orders, revenue, avg_order_value
FROM gold_ecommerce.gold_daily_sales
ORDER BY order_date DESC;
```

## 🧪 Testing

Run unit tests:

```bash
pytest tests/ -v
pytest tests/ --cov=src/
```

## 🔧 Configuration

Edit `src/config.py` to customize:

- Catalog and schema names
- Data paths
- Refresh intervals
- Auto-scaling parameters
- Alert configurations

## 📊 Monitoring

The pipeline includes monitoring capabilities:

- Data quality metrics via expectations
- Pipeline execution logs in Databricks
- Data lineage tracking
- Performance metrics

View pipeline status in Databricks UI:
- Navigate to Workflows → Delta Live Tables
- Select the pipeline
- Monitor runs and check for quality violations

## 🚨 Troubleshooting

### Issue: Data quality expectation failures
**Solution**: Check the data quality metrics in the DLT flow
- Review source data format
- Validate schema matches expectations
- Check for null or invalid values

### Issue: Pipeline execution timeout
**Solution**: Adjust cluster configuration
- Increase worker count
- Upgrade worker node types
- Monitor resource utilization

### Issue: Missing data in Gold layer
**Solution**: Verify Silver layer data exists
- Run quality checks
- Validate joins and aggregations
- Check for filtering conditions

## 📚 Additional Resources

- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)
- [Databricks SQL Guide](https://docs.databricks.com/sql/)
- [Apache Spark Documentation](https://spark.apache.org/docs/)
- [Medallion Architecture Pattern](https://www.databricks.com/blog/2022/06/24/use-the-medallion-multi-hop-architecture.html)

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👤 Author

- **Mahendra204** - Initial work on Data Engineering projects

## 📧 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Check existing issues for solutions
- Review the troubleshooting section

---

**Last Updated**: March 2026

**Version**: 1.0.0

**Status**: Production Ready
