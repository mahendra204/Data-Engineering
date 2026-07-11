# Data Engineering Framework - Comprehensive Guide

## Quick Start

```bash
# 1. Setup PostgreSQL
psql -U postgres -d metadata_management -f sql_scripts/metadata_schema.sql

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Configure credentials
cp config/config.template.yaml config/config.yaml
# Edit config.yaml with your database and AWS credentials

# 4. Run example
python examples/simple_extraction_example.py
```

---

## Architecture at a Glance

```
USER/SCHEDULER
    ↓
[PIPELINE CONTROLLER]
    ↓
SERVICE LAYER:
├─ Metadata Service → PostgreSQL
├─ Connector Service → Source Systems
├─ Transform Service → Data Processing
├─ Storage Service → AWS S3
└─ Audit Service → PostgreSQL
    ↓
DATA ACCESS LAYER:
├─ Repository (PostgreSQL)
└─ Connectors (SQL Server, Oracle, MongoDB, etc.)
```

---

## Key Design Patterns

### 1. **Metadata-Driven Architecture**
All extraction logic is defined in PostgreSQL metadata tables, NOT in code:

```
Before (Bad):
    if source == "sqlserver":
        query = "SELECT * FROM customers WHERE modified_date > ..."
        # Hardcoded logic
    elif source == "oracle":
        query = "SELECT * FROM employees WHERE updated_date > ..."

After (Good):
    metadata = MetadataService.get_pipeline(pipeline_id)
    query = metadata.extraction_query  # Loaded from DB
    # Generic execution logic
```

**Benefits:**
- Change extraction logic without deploying code
- Single execution engine for all sources
- Easy to audit and track changes
- Version control in database

### 2. **MVC Pattern**
Clear separation between orchestration (Controller), data (Model), and business logic (Service):

```
CONTROLLER (PipelineController):
  - Entry point for execution
  - Coordinates services
  - Manages workflow state
  
MODEL (Pipeline, ExecutionLog, etc.):
  - Immutable data objects
  - Type-safe configuration
  - Validation at model level
  
SERVICE LAYER:
  - Metadata Service: Configuration management
  - Connector Service: Source connectivity
  - Transform Service: Data processing
  - Storage Service: S3 operations
  - Audit Service: Logging and compliance
```

### 3. **Connector Factory Pattern**
Supports multiple source systems without hardcoding:

```python
# Register connectors
factory = ConnectorFactory()
factory.register_connector('SQLSERVER', SQLServerConnector)
factory.register_connector('ORACLE', OracleConnector)
factory.register_connector('MONGODB', MongoConnector)

# Create appropriate connector dynamically
connector = factory.create_connector(source_type, config)
```

**Supports:**
- SQL Server
- Oracle
- PostgreSQL
- MongoDB
- CSV/Excel files
- REST APIs
- SFTP

### 4. **Retry & Circuit Breaker Pattern**
Resilience against transient failures:

```python
# Exponential backoff retry
retry_strategy = RetryStrategy(
    max_attempts=5,
    initial_delay=1s,
    backoff_multiplier=2.0
)
# Attempts: 1s, 2s, 4s, 8s, 16s

# Circuit breaker for persistent failures
circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=300s
)
# After 5 failures: circuit opens
# Resets after 5 minutes of no attempts
```

### 5. **Layered S3 Storage**
Three-layer data lake architecture:

```
RAW LAYER (s3://data-lake/raw/)
├─ Purpose: Exact copy from source
├─ Format: Parquet (compressed)
├─ Retention: 90 days
└─ Access: On-demand queries

CURATED LAYER (s3://data-lake/curated/)
├─ Purpose: Cleaned, transformed data
├─ Format: Parquet (optimized)
├─ Retention: 2 years
└─ Access: Frequent analytics queries

ARCHIVE LAYER (s3://data-lake/archive/)
├─ Purpose: Historical backup
├─ Format: Compressed Parquet
├─ Retention: 7+ years
└─ Access: Rare, cold storage
```

---

## Detailed Data Flow

### Full Extraction Pipeline

```
1. REQUEST ARRIVES
   execution_request = ExecutionRequest(
       pipeline_id=1,
       execution_type='SCHEDULED',
       triggered_by='AIRFLOW'
   )

2. LOAD METADATA
   pipeline = metadata_service.get_pipeline_by_id(1)
   # Returns: Pipeline object with all config

3. VALIDATE CONFIG
   - Check all required fields
   - Validate SQL queries
   - Check S3 paths
   - Verify retention policies

4. CREATE CONNECTOR
   source_type = "SQLSERVER"
   connector = ConnectorFactory.create_connector(
       source_type,
       connection_config
   )

5. ESTABLISH CONNECTION
   connector.connect()
   connector.validate_connection()

6. EXTRACT DATA
   data = connector.execute_query(
       query="SELECT * FROM customers",
       params=None
   )
   # Returns: pandas DataFrame

7. VALIDATE DATA QUALITY
   - NULL checks
   - Data type validation
   - Range checks
   - Uniqueness validation
   - Business rule validation

8. TRANSFORM DATA
   - Type conversions
   - Standardization (formatting)
   - Enrichment (joins)
   - Deduplication
   - PII masking

9. STORE TO S3
   Storage: s3://data-lake/raw/sqlserver_prod/customers/2024-01-15/
   Format: Parquet with Snappy compression
   Partitions: By load_date, source_system

10. UPDATE METADATA
    - Insert into data_assets table
    - Update checkpoint for incremental loads
    - Record data lineage

11. LOG EXECUTION
    - Execution start/end times
    - Records processed
    - Duration and performance metrics
    - Data quality scores
    - Error details (if any)

12. RETURN RESPONSE
    ExecutionResponse(
        execution_id=12345,
        status=SUCCESS,
        total_records=100000,
        successful_records=100000,
        target_s3_location="s3://...",
        quality_score=99.8
    )
```

---

## Configuration Management

### PostgreSQL Metadata Schema

```sql
-- Define source system
INSERT INTO metadata_sources (source_name, source_type, status)
VALUES ('SQLSERVER_PROD', 'SQLSERVER', 'ACTIVE');

-- Define connection
INSERT INTO metadata_connections (
    source_id, connection_name, environment, host, database_name
)
VALUES (1, 'PROD', 'PROD', 'sqlserver-prod.internal', 'CustomerDB');

-- Define source table
INSERT INTO metadata_source_tables (
    connection_id, source_table_name, is_incremental, cdc_column
)
VALUES (1, 'customers', true, 'modified_date');

-- Define field mappings
INSERT INTO metadata_fields (
    table_id, source_field_name, target_field_name,
    source_data_type, target_data_type
)
VALUES
    (1, 'customer_id', 'customer_id', 'INT', 'BIGINT'),
    (1, 'email', 'email', 'VARCHAR', 'VARCHAR'),
    (1, 'created_date', 'created_date', 'DATETIME', 'DATE');

-- Define data quality rules
INSERT INTO metadata_validations (
    table_id, validation_name, validation_type,
    target_field, threshold, severity
)
VALUES (1, 'customer_id_not_null', 'NULL_CHECK', 'customer_id', 0.0, 'HIGH');

-- Define pipeline
INSERT INTO metadata_pipelines (
    pipeline_name, source_id, table_id, connection_id,
    extraction_type, extraction_query, target_s3_prefix
)
VALUES (
    'SQLSERVER_CUSTOMERS_DAILY',
    1, 1, 1,
    'FULL',
    'SELECT * FROM customers',
    's3://data-lake/raw/sqlserver_prod/customers/'
);
```

---

## Security Features

### Database Protection
- ✅ No direct connection strings in code
- ✅ Encrypted credentials in PostgreSQL
- ✅ Parameterized queries (SQL injection prevention)
- ✅ Connection pooling with timeouts
- ✅ IAM roles for AWS services

### Data Protection
- ✅ PII masking configuration in metadata
- ✅ Field-level encryption for sensitive columns
- ✅ Encryption in transit (TLS/SSL)
- ✅ Encryption at rest (S3 KMS)

### Audit & Compliance
- ✅ Complete execution audit trail
- ✅ Data lineage tracking
- ✅ User access logging
- ✅ Change history in metadata tables

---

## Monitoring & Observability

### Key Metrics to Track

```
Per Execution:
├─ Total execution time (ms)
├─ Records extracted
├─ Records transformed
├─ Records failed
├─ Data quality score (0-100)
├─ Average records/second
└─ Memory usage

Per Pipeline:
├─ Success rate (%)
├─ Average duration
├─ Data quality trend
├─ Failure count (last 7 days)
└─ Last execution timestamp
```

### Query Examples

```sql
-- Find slow executions
SELECT * FROM execution_log
WHERE pipeline_id = 1
ORDER BY total_execution_time_ms DESC
LIMIT 10;

-- Find data quality issues
SELECT * FROM data_quality_log
WHERE quality_status != 'PASSED'
ORDER BY created_at DESC;

-- Pipeline success rate (last 7 days)
SELECT 
    pipeline_id,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM execution_log
WHERE started_at >= NOW() - INTERVAL '7 days'
GROUP BY pipeline_id;
```

---

## Troubleshooting Guide

### Connection Fails

```
Error: "Connection timeout to SQLSERVER_PROD"

Solution:
1. Check PostgreSQL metadata:
   SELECT * FROM metadata_connections WHERE connection_id = X;

2. Verify network connectivity:
   telnet sqlserver-prod.internal 1433

3. Check credentials are encrypted and valid

4. Examine error log:
   SELECT * FROM error_log WHERE error_type = 'CONNECTION_ERROR'
   ORDER BY created_at DESC LIMIT 1;

5. Check circuit breaker state (if many failures):
   - Wait for recovery timeout (5 minutes default)
   - Or restart service to reset
```

### Data Quality Failures

```
Error: "Data quality checks failed: customer_id_not_null"

Solution:
1. Review validation rule:
   SELECT * FROM metadata_validations
   WHERE validation_name = 'customer_id_not_null';

2. Check source data:
   SELECT COUNT(*) WHERE customer_id IS NULL FROM source_table;

3. Adjust threshold if needed (increase from 0.0% to 1.0%):
   UPDATE metadata_validations SET threshold = 1.0
   WHERE validation_name = 'customer_id_not_null';

4. Or change action to 'QUARANTINE' instead of 'REJECT'
```

### Slow Extraction

```
Execution taking longer than expected

Solution:
1. Check execution metrics:
   SELECT * FROM performance_metrics
   WHERE execution_id = X;

2. Identify bottleneck:
   - Connection time: Network issue?
   - Query execution time: Complex query? Large table?
   - Transformation time: Many transformations?
   - S3 upload time: Network/S3 performance?

3. Optimize:
   - Add batch processing with pagination
   - Reduce transformation complexity
   - Adjust partition strategy
   - Increase connection pool size
```

---

## Best Practices

### 1. **Pipeline Design**
- ✓ Start with FULL extraction to establish baseline
- ✓ Switch to INCREMENTAL once baseline is established
- ✓ Use CDC when available for minimal data movement
- ✓ Partition data by date + source system

### 2. **Metadata Management**
- ✓ Document purpose of each pipeline
- ✓ Update metadata as business rules change
- ✓ Keep transformation logic in metadata, not code
- ✓ Version control changes to metadata

### 3. **Error Handling**
- ✓ Configure appropriate retry strategies
- ✓ Set realistic thresholds for data quality
- ✓ Monitor circuit breaker state
- ✓ Alert on repeated failures

### 4. **Performance**
- ✓ Monitor execution metrics
- ✓ Adjust batch sizes based on data size
- ✓ Use appropriate file formats (Parquet > CSV)
- ✓ Partition large datasets

### 5. **Security**
- ✓ Rotate credentials regularly
- ✓ Use service accounts for extractions
- ✓ Enable encryption for sensitive data
- ✓ Review audit logs periodically

---

## Next Steps

1. **Setup Phase** (Week 1-2)
   - [ ] Create PostgreSQL metadata database
   - [ ] Configure AWS S3 buckets
   - [ ] Set up IAM roles and policies
   - [ ] Install framework in development environment

2. **Development Phase** (Week 3-6)
   - [ ] Create pipelines for existing source systems
   - [ ] Configure data quality rules
   - [ ] Test error handling and recovery
   - [ ] Develop monitoring dashboards

3. **Deployment Phase** (Week 7-8)
   - [ ] Deploy to production environment
   - [ ] Configure scheduling (Airflow, Lambda, etc.)
   - [ ] Set up alerting
   - [ ] Train team on operations

4. **Optimization Phase** (Ongoing)
   - [ ] Monitor performance metrics
   - [ ] Optimize slow pipelines
   - [ ] Add new data sources
   - [ ] Refine data quality rules

---

## Support & Resources

- **Architecture Documentation**: [ARCHITECTURE_OVERVIEW.md](docs/ARCHITECTURE_OVERVIEW.md)
- **Workflow Guide**: [WORKFLOW_GUIDE.md](docs/WORKFLOW_GUIDE.md)
- **Database Schema**: [sql_scripts/metadata_schema.sql](sql_scripts/metadata_schema.sql)
- **Python Examples**: [examples/](examples/)

---

**Framework Version**: 1.0  
**Last Updated**: 2024  
**Status**: Production Ready
