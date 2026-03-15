# Pipeline Configuration Guide

## DLT Pipeline Configuration

This guide explains how to configure and deploy the Delta Live Tables pipeline in Databricks.

## Configuration Files

### 1. config.py

Main configuration file containing:

```python
# Pipeline properties
PIPELINE_NAME = "ecommerce_declarative_pipeline"
PIPELINE_VERSION = "1.0.0"

# Path configurations
SOURCE_DATA_PATH = "/dbfs/path/to/source/data"
BRONZE_PATH = "/dbfs/user/hive/warehouse/bronze"
SILVER_PATH = "/dbfs/user/hive/warehouse/silver"
GOLD_PATH = "/dbfs/user/hive/warehouse/gold"
```

### 2. dlt_pipeline.py

Defines the transformation logic using decorators:

```python
@dlt.table(comment="...", table_properties={...})
def table_name():
    # Transformation logic
    return spark.read...
```

## Deployment

### Step 1: Upload Files to Databricks

```bash
# Using Databricks CLI
databricks workspace import ./src/dlt_pipeline.py /Shared/pipelines/dlt_pipeline.py --language PYTHON

# Or manually upload via UI
# Workspace → Shared → Create Folder → Upload Files
```

### Step 2: Create DLT Pipeline

In Databricks UI:

1. Go to Workflows → Delta Live Tables
2. Click "Create pipeline"
3. Configure:
   - **Pipeline Name**: ecommerce_declarative_pipeline
   - **Source Code Path**: /Shared/pipelines/dlt_pipeline.py
   - **Storage Location**: /user/hive/warehouse/dlt_pipelines
   - **Cluster Configuration**: Select compute

### Step 3: Configure Cluster

**Development:**
```
Cluster Type: Single Node
Node Type: i3.xlarge
Runtime: 13.3 LTS
```

**Production:**
```
Cluster Type: Multi Node
Driver: i3.xlarge
Workers: 2-8 (with auto-scale)
Runtime: 13.3 LTS
Photon: Enabled (optional)
```

### Step 4: Run Pipeline

1. Click "Start" in the DLT pipeline UI
2. Monitor execution in the DAG view
3. Check Data Quality expectations
4. Review run logs

## Monitoring

### Data Quality

- Check expectations in the UI
- Failed expectations show in red
- Click for detailed error information

### Performance

- Monitor execution time
- Check cluster resource utilization
- Review Spark UI for optimization opportunities

### Logs

- Access logs from pipeline run details
- Check driver and executor logs
- Review Delta transaction logs in storage

## Troubleshooting

### Common Issues

1. **File not found error**
   - Verify DBFS paths are correct
   - Check file permissions
   - Ensure correct path format

2. **Data type mismatches**
   - Review schema definitions
   - Check data format in source files
   - Validate cast operations

3. **Permission denied**
   - Check workspace permissions
   - Verify cluster access
   - Review storage credentials

## Advanced Configuration

### Custom Spark Configuration

Add to cluster configuration:

```
spark.databricks.adaptive.execution.enabled true
spark.databricks.optimizer.adaptive.skewJoin.enabled true
spark.sql.adaptive.skewJoin.skewFactor 3.0
```

### Notification Configuration

Set up alerts via:

- Webhook integrations
- Email notifications
- Slack channels

### Partitioning Strategy

Configure partitions in config.py:

```python
PARTITION_COLUMNS = {
    "orders": "order_date",
    "customers": None,  # No partitioning
    "products": None
}
```

## Production Deployment Checklist

- [ ] Data paths configured correctly
- [ ] Cluster sizing appropriate for data volume
- [ ] Data quality expectations defined
- [ ] Monitoring alerts configured
- [ ] Backup and disaster recovery plan
- [ ] Documentation updated
- [ ] Testing completed
- [ ] Access controls configured

## Support

For issues or questions:
1. Check pipeline run logs
2. Review data quality expectations
3. Consult Databricks documentation
4. Open a GitHub issue
