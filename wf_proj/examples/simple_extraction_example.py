# ============================================================================
# EXAMPLE: SIMPLE EXTRACTION PIPELINE
# Demonstrates how to use the framework
# ============================================================================

"""
Example: Extract Customer Orders from SQL Server to S3

This example demonstrates:
1. Loading pipeline configuration from PostgreSQL metadata
2. Creating a SQL connector
3. Extracting data with retries
4. Transforming data
5. Storing to S3 in RAW layer
6. Logging execution metrics
"""

from datetime import datetime
import logging

# Framework imports (in real scenario, installed as package)
from framework.models.base_models import (
    ExecutionRequest, PipelineStatus, TargetLayer
)
from framework.controllers.pipeline_controller import PipelineController
from framework.services.base_service import ServiceLogger


# ============================================================================
# SETUP LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# ============================================================================
# EXAMPLE 1: FULL LOAD FROM SQL SERVER
# ============================================================================

def example_sql_server_full_load():
    """
    Scenario: Extract all customer records from SQL Server
    
    Pipeline Configuration (in PostgreSQL):
    - Source: SQL_SERVER_PROD
    - Table: customers
    - Query: SELECT * FROM dbo.customers
    - Target: s3://data-lake/raw/sqlserver_prod/customers/[load_date]/
    - Format: Parquet with Snappy compression
    """
    
    logger = ServiceLogger("ExampleSQLServerFullLoad")
    logger.info("Starting SQL Server full load example")
    
    # Configuration loaded from PostgreSQL metadata
    pipeline_config = {
        'pipeline_id': 1,
        'pipeline_name': 'SQLSERVER_CUSTOMERS_DAILY_LOAD',
        'source_type': 'SQLSERVER',
        'extraction_type': 'FULL',
        'extraction_query': 'SELECT * FROM dbo.customers',
        'connection': {
            'host': 'sqlserver-prod.internal',
            'port': 1433,
            'database_name': 'CustomerDB',
            'username': 'svc_data_extract',  # Service account
            'password_encrypted': '***',  # Encrypted in production
            'connection_timeout': 30,
            'pool_size': 10
        },
        'target': {
            'bucket': 'data-lake',
            'layer': 'RAW',
            'path': 'raw/sqlserver_prod/customers/'
        },
        'transformations': [
            {'type': 'TRIM_WHITESPACE', 'columns': ['*']},
            {'type': 'STANDARDIZE_DATE', 'columns': ['created_date'], 'format': 'YYYY-MM-DD'},
            {'type': 'UPPERCASE', 'columns': ['country_code']}
        ],
        'validations': [
            {'field': 'customer_id', 'rule': 'NOT NULL', 'action': 'REJECT'},
            {'field': 'email', 'rule': 'REGEX ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', 'action': 'QUARANTINE'},
            {'field': 'customer_id', 'rule': 'UNIQUE', 'threshold': 100}
        ]
    }
    
    # Create execution request
    execution_request = ExecutionRequest(
        pipeline_id=pipeline_config['pipeline_id'],
        execution_type='SCHEDULED',
        triggered_by='AIRFLOW_SCHEDULER'
    )
    
    logger.info(f"Execution request created: {execution_request}")
    
    # In production, you would initialize services:
    # controller = PipelineController(
    #     metadata_service=MetadataService(db_connection),
    #     storage_service=S3StorageService(aws_credentials),
    #     transform_service=TransformationService(),
    #     validation_service=DataValidationService(),
    #     audit_service=AuditService(db_connection)
    # )
    #
    # response = controller.execute_pipeline(execution_request)
    # 
    # if response.status == PipelineStatus.SUCCESS:
    #     logger.info(f"Extraction successful!")
    #     logger.info(f"  Records: {response.total_records}")
    #     logger.info(f"  Location: {response.target_s3_location}")
    #     logger.info(f"  Quality Score: {response.quality_score}")
    #     logger.info(f"  Execution Time: {response.metrics.total_execution_time_ms}ms")
    # else:
    #     logger.error(f"Extraction failed: {response.error_message}")
    
    print("\n✓ SQL Server Full Load Example Configuration Ready")
    print("  Pipeline ID: 1")
    print("  Expected Records: ~100,000")
    print("  Expected Duration: ~5 minutes")
    print("  Target: s3://data-lake/raw/sqlserver_prod/customers/")


# ============================================================================
# EXAMPLE 2: INCREMENTAL LOAD FROM ORACLE
# ============================================================================

def example_oracle_incremental_load():
    """
    Scenario: Extract changed customer records from Oracle
    
    Pipeline Configuration:
    - Source: ORACLE_HR
    - Table: employees
    - Extraction Type: INCREMENTAL
    - CDC Column: modified_date
    - Query: SELECT * FROM employees WHERE modified_date > :last_timestamp
    - Target: s3://data-lake/curated/oracle_hr/employees/[load_date]/
    """
    
    logger = ServiceLogger("ExampleOracleIncrementalLoad")
    logger.info("Starting Oracle incremental load example")
    
    pipeline_config = {
        'pipeline_id': 2,
        'pipeline_name': 'ORACLE_EMPLOYEES_HOURLY_INCREMENTAL',
        'source_type': 'ORACLE',
        'extraction_type': 'INCREMENTAL',
        'cdc_column': 'modified_date',
        'extraction_query': '''
            SELECT * FROM employees 
            WHERE modified_date > :last_timestamp
            ORDER BY employee_id
        ''',
        'connection': {
            'host': 'oracle-prod.internal',
            'port': 1521,
            'database_name': 'HR',
            'username': 'svc_data_extract',
            'password_encrypted': '***'
        },
        'target': {
            'bucket': 'data-lake',
            'layer': 'CURATED',
            'path': 'curated/oracle_hr/employees/'
        },
        'batch_size': 50000,  # Process in 50K record chunks
        'partition_columns': ['department_id', 'load_date']
    }
    
    execution_request = ExecutionRequest(
        pipeline_id=pipeline_config['pipeline_id'],
        execution_type='SCHEDULED',
        triggered_by='AIRFLOW_SCHEDULER'
    )
    
    print("\n✓ Oracle Incremental Load Example Configuration Ready")
    print("  Pipeline ID: 2")
    print("  Frequency: Hourly")
    print("  Partition Strategy: By Department and Load Date")


# ============================================================================
# EXAMPLE 3: MONGODB TO S3 WITH TRANSFORMATIONS
# ============================================================================

def example_mongodb_to_s3():
    """
    Scenario: Extract customer profiles from MongoDB
    
    Special considerations:
    - Flatten nested documents
    - Convert ObjectIds to strings
    - Apply PII masking for phone numbers
    - Deduplicate on customer_id
    """
    
    logger = ServiceLogger("ExampleMongoDBExtraction")
    logger.info("Starting MongoDB extraction example")
    
    pipeline_config = {
        'pipeline_id': 3,
        'pipeline_name': 'MONGODB_CUSTOMER_PROFILES_DAILY',
        'source_type': 'MONGODB',
        'extraction_type': 'FULL',
        'extraction_query': 'customer_profiles',  # Collection name
        'connection': {
            'host': 'mongodb-cluster.internal',
            'port': 27017,
            'database_name': 'crm',
            'username': 'svc_data_extract',
            'password_encrypted': '***'
        },
        'target': {
            'bucket': 'data-lake',
            'layer': 'RAW',
            'path': 'raw/mongodb_crm/customer_profiles/'
        },
        'transformations': [
            {
                'type': 'FLATTEN_JSON',
                'config': {'flatten_nested': True, 'max_depth': 2}
            },
            {
                'type': 'PII_MASK',
                'columns': {
                    'phone_number': 'HASH',
                    'ssn': 'REDACT',
                    'credit_card': 'TRUNCATE'
                }
            },
            {
                'type': 'DEDUPLICATION',
                'key_columns': ['customer_id'],
                'keep': 'LATEST'
            }
        ]
    }
    
    print("\n✓ MongoDB Extraction Example Configuration Ready")
    print("  Pipeline ID: 3")
    print("  Collection: customer_profiles")
    print("  Transformations: PII Masking + Deduplication")


# ============================================================================
# EXAMPLE 4: ERROR HANDLING & RECOVERY
# ============================================================================

def example_error_handling():
    """
    Scenario: Demonstrating error handling and recovery mechanisms
    
    This example shows how the framework handles:
    1. Connection failures with exponential backoff
    2. Data quality violations
    3. Partial extraction recovery
    4. Comprehensive error logging
    """
    
    logger = ServiceLogger("ExampleErrorHandling")
    logger.info("Starting error handling example")
    
    # Scenario: Connection fails initially, then succeeds on retry
    print("\n✓ Error Handling Scenarios:")
    print("  1. Connection Timeout")
    print("     - Initial attempt fails")
    print("     - Retry #1: Wait 1s, attempt")
    print("     - Retry #2: Wait 2s, attempt")
    print("     - Retry #3: Wait 4s, SUCCESS ✓")
    print("")
    print("  2. Data Quality Violation")
    print("     - Validation Rule: customer_id NOT NULL")
    print("     - Violations: 500 records (0.5% of 100K)")
    print("     - Threshold: 5%")
    print("     - Action: QUARANTINE (records saved to dead-letter)")
    print("     - Extraction: CONTINUES with 99.5% valid records ✓")
    print("")
    print("  3. Partial Extraction Failure")
    print("     - Extracted 50M of 100M records")
    print("     - Connection dropped")
    print("     - Checkpoint stored: offset = 50,000,000")
    print("     - Next execution: Resumes from checkpoint")
    print("     - Deduplicates already-processed records")
    print("     - No re-processing of first 50M ✓")
    print("")
    print("  4. Circuit Breaker")
    print("     - 5 consecutive connection failures")
    print("     - Circuit switches to OPEN state")
    print("     - Subsequent calls fail fast (no retry)")
    print("     - After 5 minute cooldown: Attempts recovery")


# ============================================================================
# EXAMPLE 5: SCHEDULED PIPELINE EXECUTION
# ============================================================================

def example_airflow_integration():
    """
    Scenario: Integration with Apache Airflow for scheduling
    
    This shows how to integrate the framework with Airflow DAG
    """
    
    example_dag_code = '''
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime, timedelta
    from framework.models.base_models import ExecutionRequest
    from framework.controllers.pipeline_controller import PipelineController
    
    default_args = {
        'owner': 'data-engineering',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': notify_failure
    }
    
    dag = DAG(
        dag_id='data_extraction_pipeline',
        default_args=default_args,
        schedule_interval='0 2 * * *',  # Daily at 2 AM
        start_date=datetime(2024, 1, 1),
        catchup=False
    )
    
    def execute_extraction(**context):
        execution_request = ExecutionRequest(
            pipeline_id=int(context['params']['pipeline_id']),
            execution_type='SCHEDULED',
            triggered_by='AIRFLOW'
        )
        
        controller = PipelineController(
            metadata_service=MetadataService(),
            storage_service=S3StorageService(),
            transform_service=TransformationService(),
            validation_service=DataValidationService(),
            audit_service=AuditService()
        )
        
        response = controller.execute_pipeline(execution_request)
        return response
    
    # Task for SQL Server extraction
    extract_sqlserver = PythonOperator(
        task_id='extract_sqlserver_customers',
        python_callable=execute_extraction,
        params={'pipeline_id': 1},
        dag=dag
    )
    
    # Task for Oracle extraction
    extract_oracle = PythonOperator(
        task_id='extract_oracle_employees',
        python_callable=execute_extraction,
        params={'pipeline_id': 2},
        dag=dag
    )
    
    # Tasks can run in parallel
    [extract_sqlserver, extract_oracle]
    '''
    
    print("\n✓ Airflow Integration Example:")
    print("  DAG: data_extraction_pipeline")
    print("  Schedule: Daily at 2 AM UTC")
    print("  Tasks: Run in parallel for multiple pipelines")
    print("  Error Handling: Retry on failure + notifications")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Run all examples"""
    
    print("=" * 80)
    print("DATA ENGINEERING FRAMEWORK - PRACTICAL EXAMPLES")
    print("=" * 80)
    
    example_sql_server_full_load()
    example_oracle_incremental_load()
    example_mongodb_to_s3()
    example_error_handling()
    example_airflow_integration()
    
    print("\n" + "=" * 80)
    print("All examples configured successfully!")
    print("=" * 80)


if __name__ == '__main__':
    main()
