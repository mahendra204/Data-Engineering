-- ============================================================================
-- METADATA MANAGEMENT DATABASE SCHEMA
-- PostgreSQL DDL Scripts
-- ============================================================================

-- ============================================================================
-- 1. CONFIGURATION TABLES
-- ============================================================================

-- 1.1 Source System Registry
CREATE TABLE IF NOT EXISTS metadata_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL,  -- SQLSERVER, ORACLE, MONGODB, SFTP, API, FILE
    description TEXT,
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE, INACTIVE, DEPRECATED
    documentation_url VARCHAR(500),
    owner_team VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- 1.2 Connection Credentials
CREATE TABLE IF NOT EXISTS metadata_connections (
    connection_id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES metadata_sources(source_id),
    connection_name VARCHAR(100) NOT NULL,
    environment VARCHAR(20) NOT NULL,  -- DEV, UAT, PROD
    host VARCHAR(255) NOT NULL,
    port INTEGER,
    database_name VARCHAR(100),
    username VARCHAR(100),
    password_encrypted VARCHAR(500),  -- Encrypted value
    encryption_key_id VARCHAR(100),  -- Reference to encryption key
    connection_string TEXT,  -- For complex connection strings
    additional_params JSONB,  -- Custom parameters as JSON
    connection_timeout INTEGER DEFAULT 30,  -- seconds
    pool_size INTEGER DEFAULT 10,
    pool_timeout INTEGER DEFAULT 60,
    validation_query VARCHAR(500),  -- Query to validate connection
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100),
    UNIQUE(source_id, connection_name, environment)
);

-- 1.3 Source Tables/Objects Registry
CREATE TABLE IF NOT EXISTS metadata_source_tables (
    table_id SERIAL PRIMARY KEY,
    connection_id INTEGER NOT NULL REFERENCES metadata_connections(connection_id),
    source_table_name VARCHAR(255) NOT NULL,  -- Actual table name in source
    display_name VARCHAR(255),
    table_type VARCHAR(20),  -- TABLE, VIEW, STORED_PROCEDURE, API_ENDPOINT
    description TEXT,
    estimated_row_count BIGINT,
    last_row_count BIGINT,
    last_row_count_date TIMESTAMP,
    primary_key_columns VARCHAR(500),  -- Comma-separated
    is_incremental BOOLEAN DEFAULT FALSE,
    cdc_enabled BOOLEAN DEFAULT FALSE,
    cdc_column VARCHAR(100),  -- CDC column (e.g., modified_date, SCN)
    extraction_frequency VARCHAR(50),  -- DAILY, HOURLY, REALTIME
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(connection_id, source_table_name)
);

-- 1.4 Field Mappings
CREATE TABLE IF NOT EXISTS metadata_fields (
    field_id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES metadata_source_tables(table_id),
    source_field_name VARCHAR(255) NOT NULL,
    target_field_name VARCHAR(255) NOT NULL,
    source_data_type VARCHAR(50),
    target_data_type VARCHAR(50),
    field_length INTEGER,
    is_key_field BOOLEAN DEFAULT FALSE,
    is_required BOOLEAN DEFAULT FALSE,
    is_masked BOOLEAN DEFAULT FALSE,  -- PII masking
    masking_type VARCHAR(50),  -- HASH, TRUNCATE, REDACT, SUBSTITUTE
    transformation_expression VARCHAR(1000),  -- DAX/SQL transformation
    validation_rule VARCHAR(1000),  -- Custom validation rule
    default_value VARCHAR(255),
    field_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.5 Data Quality Rules
CREATE TABLE IF NOT EXISTS metadata_validations (
    validation_id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES metadata_source_tables(table_id),
    validation_name VARCHAR(255) NOT NULL,
    validation_type VARCHAR(50),  -- NULL_CHECK, RANGE_CHECK, REGEX, UNIQUENESS, REF_INTEGRITY, CUSTOM
    target_field VARCHAR(100),
    rule_expression VARCHAR(2000),  -- SQL or Python expression
    threshold DECIMAL(5,2),  -- Acceptable percentage of violations (e.g., 5.0 = 5%)
    action_on_failure VARCHAR(50),  -- REJECT, QUARANTINE, FLAG, WARN
    severity VARCHAR(20),  -- HIGH, MEDIUM, LOW
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.6 Transformations
CREATE TABLE IF NOT EXISTS metadata_transformations (
    transformation_id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES metadata_source_tables(table_id),
    transformation_name VARCHAR(255) NOT NULL,
    transformation_type VARCHAR(50),  -- TYPE_CONVERT, JOIN, AGGREGATE, FILTER, PII_MASK, STANDARDIZE, CUSTOM_PYTHON
    sequence_number INTEGER NOT NULL,
    transformation_sql VARCHAR(5000),  -- For SQL-based transforms
    transformation_python VARCHAR(5000),  -- For Python-based transforms
    description TEXT,
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.7 Pipeline Configuration
CREATE TABLE IF NOT EXISTS metadata_pipelines (
    pipeline_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL UNIQUE,
    source_id INTEGER NOT NULL REFERENCES metadata_sources(source_id),
    table_id INTEGER NOT NULL REFERENCES metadata_source_tables(table_id),
    connection_id INTEGER NOT NULL REFERENCES metadata_connections(connection_id),
    extraction_type VARCHAR(50) NOT NULL,  -- FULL, INCREMENTAL, CDC, API_PAGINATED
    extraction_query VARCHAR(5000),  -- Custom SQL query if not standard
    extraction_method VARCHAR(50),  -- QUERY, FILE_READ, API_CALL, STORED_PROCEDURE
    target_s3_prefix VARCHAR(500) NOT NULL,  -- s3://bucket/[layer]/[source]/[table]
    target_layer VARCHAR(50) NOT NULL,  -- RAW, CURATED, ARCHIVE
    partition_columns VARCHAR(500),  -- Comma-separated columns for partitioning
    file_format VARCHAR(20) DEFAULT 'PARQUET',  -- PARQUET, ORC, CSV, JSON
    compression_type VARCHAR(20) DEFAULT 'SNAPPY',  -- SNAPPY, GZIP, NONE
    batch_size INTEGER DEFAULT 100000,  -- Records per batch
    chunk_size_mb INTEGER DEFAULT 256,  -- Target file size
    is_incremental BOOLEAN DEFAULT FALSE,
    last_extraction_timestamp TIMESTAMP,
    cdc_last_scn VARCHAR(100),  -- For CDC tracking
    data_retention_days INTEGER DEFAULT 90,  -- Raw layer retention
    is_enabled BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- 1.8 Field Masking Definitions
CREATE TABLE IF NOT EXISTS metadata_field_masking (
    masking_id SERIAL PRIMARY KEY,
    field_id INTEGER NOT NULL REFERENCES metadata_fields(field_id),
    masking_type VARCHAR(50) NOT NULL,  -- HASH, TRUNCATE, SUBSTITUTE, REDACT
    masking_pattern VARCHAR(255),
    masking_key VARCHAR(255),
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.9 Dimensions and Reference Data
CREATE TABLE IF NOT EXISTS metadata_dimensions (
    dimension_id SERIAL PRIMARY KEY,
    dimension_name VARCHAR(255) NOT NULL UNIQUE,
    connection_id INTEGER NOT NULL REFERENCES metadata_connections(connection_id),
    source_table_name VARCHAR(255) NOT NULL,
    key_column VARCHAR(100) NOT NULL,
    display_column VARCHAR(100),
    is_cached BOOLEAN DEFAULT TRUE,
    cache_ttl_hours INTEGER DEFAULT 24,
    last_cache_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 2. EXECUTION TRACKING TABLES
-- ============================================================================

-- 2.1 Execution Log
CREATE TABLE IF NOT EXISTS execution_log (
    execution_id BIGSERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES metadata_pipelines(pipeline_id),
    execution_type VARCHAR(50),  -- SCHEDULED, MANUAL, API_TRIGGER, RETRY
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(50) NOT NULL,  -- RUNNING, SUCCESS, FAILED, PARTIAL, QUARANTINED
    total_records BIGINT DEFAULT 0,
    successful_records BIGINT DEFAULT 0,
    failed_records BIGINT DEFAULT 0,
    skipped_records BIGINT DEFAULT 0,
    duration_seconds INTEGER,
    source_system VARCHAR(100),
    source_table VARCHAR(255),
    target_s3_location VARCHAR(500),
    execution_status_message VARCHAR(1000),
    triggered_by VARCHAR(100),
    retry_count INTEGER DEFAULT 0,
    parent_execution_id BIGINT REFERENCES execution_log(execution_id),
    data_quality_score DECIMAL(5,2),  -- 0-100
    created_by VARCHAR(100),
    INDEX idx_pipeline_date (pipeline_id, started_at DESC),
    INDEX idx_status_date (status, started_at DESC)
);

-- 2.2 Execution Details
CREATE TABLE IF NOT EXISTS execution_details (
    detail_id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL REFERENCES execution_log(execution_id),
    step_number INTEGER,
    step_name VARCHAR(255),  -- VALIDATE_CONFIG, CONNECT, EXTRACT, TRANSFORM, VALIDATE, STORE
    step_status VARCHAR(50),  -- SUCCESS, FAILED, SKIPPED
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    records_processed BIGINT,
    error_message VARCHAR(2000),
    retry_count INTEGER DEFAULT 0,
    metrics_json JSONB  -- Custom metrics per step
);

-- 2.3 Error Log
CREATE TABLE IF NOT EXISTS error_log (
    error_id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL REFERENCES execution_log(execution_id),
    error_type VARCHAR(100),  -- CONNECTION_ERROR, QUERY_ERROR, VALIDATION_ERROR, etc.
    error_message VARCHAR(2000),
    error_stack_trace TEXT,
    error_context VARCHAR(1000),  -- Where in process error occurred
    severity VARCHAR(20),  -- CRITICAL, ERROR, WARNING, INFO
    is_recoverable BOOLEAN DEFAULT FALSE,
    recovery_action VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.4 Data Quality Log
CREATE TABLE IF NOT EXISTS data_quality_log (
    quality_log_id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL REFERENCES execution_log(execution_id),
    table_id INTEGER NOT NULL REFERENCES metadata_source_tables(table_id),
    validation_id INTEGER REFERENCES metadata_validations(validation_id),
    validation_name VARCHAR(255),
    field_name VARCHAR(255),
    violation_count BIGINT,
    violation_percentage DECIMAL(5,2),
    violation_samples VARCHAR(1000),  -- Sample of violations
    quality_status VARCHAR(50),  -- PASSED, FAILED, QUARANTINED
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.5 Data Lineage
CREATE TABLE IF NOT EXISTS data_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL REFERENCES execution_log(execution_id),
    source_system VARCHAR(100),
    source_table VARCHAR(255),
    source_query_hash VARCHAR(64),  -- SHA256 hash of query
    target_s3_location VARCHAR(500),
    transformation_applied VARCHAR(500),  -- List of transformations
    data_version VARCHAR(50),
    lineage_json JSONB,  -- Detailed lineage as JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 3. OPERATIONAL TABLES
-- ============================================================================

-- 3.1 Pipeline State
CREATE TABLE IF NOT EXISTS pipeline_state (
    state_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES metadata_pipelines(pipeline_id),
    is_paused BOOLEAN DEFAULT FALSE,
    is_locked BOOLEAN DEFAULT FALSE,
    lock_reason VARCHAR(500),
    last_successful_execution_id BIGINT REFERENCES execution_log(execution_id),
    last_successful_run_time TIMESTAMP,
    last_failed_execution_id BIGINT REFERENCES execution_log(execution_id),
    consecutive_failures INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(pipeline_id)
);

-- 3.2 Pipeline Notifications
CREATE TABLE IF NOT EXISTS pipeline_notifications (
    notification_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES metadata_pipelines(pipeline_id),
    notification_type VARCHAR(50),  -- EMAIL, SLACK, WEBHOOK, TEAMS
    recipients VARCHAR(500),  -- Comma-separated email/usernames
    notify_on_success BOOLEAN DEFAULT FALSE,
    notify_on_failure BOOLEAN DEFAULT TRUE,
    notify_on_data_quality_issue BOOLEAN DEFAULT TRUE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3.3 Checkpoint/Recovery Points
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES metadata_pipelines(pipeline_id),
    checkpoint_type VARCHAR(50),  -- FULL_LOAD, INCREMENTAL, CDC
    last_processed_offset VARCHAR(255),  -- For incremental processing
    last_processed_timestamp TIMESTAMP,
    last_processed_scn VARCHAR(100),  -- For Oracle CDC
    checkpoint_data JSONB,  -- Additional checkpoint metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(pipeline_id)
);

-- 3.4 Data Assets Registry
CREATE TABLE IF NOT EXISTS data_assets (
    asset_id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL REFERENCES execution_log(execution_id),
    asset_name VARCHAR(255),
    source_system VARCHAR(100),
    source_table VARCHAR(255),
    s3_location VARCHAR(500) NOT NULL,
    layer VARCHAR(50),  -- RAW, CURATED, ARCHIVE
    record_count BIGINT,
    file_count INTEGER,
    total_size_mb DECIMAL(12,2),
    data_version VARCHAR(50),
    manifest_location VARCHAR(500),
    lifecycle_stage VARCHAR(50),  -- ACTIVE, ARCHIVED, DELETED
    scheduled_deletion_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed_at TIMESTAMP,
    created_by VARCHAR(100)
);

-- 3.5 Performance Metrics
CREATE TABLE IF NOT EXISTS performance_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL REFERENCES execution_log(execution_id),
    pipeline_id INTEGER NOT NULL REFERENCES metadata_pipelines(pipeline_id),
    connection_time_ms INTEGER,
    query_execution_time_ms INTEGER,
    data_transformation_time_ms INTEGER,
    s3_upload_time_ms INTEGER,
    total_execution_time_ms INTEGER,
    records_per_second DECIMAL(10,2),
    average_batch_time_ms INTEGER,
    memory_used_mb DECIMAL(10,2),
    cpu_used_percent DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 4. INDEXES FOR PERFORMANCE
-- ============================================================================

CREATE INDEX idx_metadata_sources_type ON metadata_sources(source_type);
CREATE INDEX idx_metadata_connections_source ON metadata_connections(source_id);
CREATE INDEX idx_metadata_connections_active ON metadata_connections(is_active);
CREATE INDEX idx_metadata_source_tables_connection ON metadata_source_tables(connection_id);
CREATE INDEX idx_metadata_fields_table ON metadata_fields(table_id);
CREATE INDEX idx_metadata_validations_table ON metadata_validations(table_id);
CREATE INDEX idx_metadata_transformations_table ON metadata_transformations(table_id);
CREATE INDEX idx_metadata_pipelines_source ON metadata_pipelines(source_id);
CREATE INDEX idx_metadata_pipelines_enabled ON metadata_pipelines(is_enabled);
CREATE INDEX idx_execution_log_pipeline ON execution_log(pipeline_id);
CREATE INDEX idx_execution_log_status ON execution_log(status);
CREATE INDEX idx_execution_log_date ON execution_log(started_at DESC);
CREATE INDEX idx_execution_details_execution ON execution_details(execution_id);
CREATE INDEX idx_error_log_execution ON error_log(execution_id);
CREATE INDEX idx_error_log_severity ON error_log(severity);
CREATE INDEX idx_data_quality_log_execution ON data_quality_log(execution_id);
CREATE INDEX idx_data_assets_s3_location ON data_assets(s3_location);
CREATE INDEX idx_data_assets_layer ON data_assets(layer);

-- ============================================================================
-- 5. GRANTS (adjust as needed for your environment)
-- ============================================================================

-- CREATE ROLE data_engineer_role;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO data_engineer_role;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO data_engineer_role;

-- CREATE ROLE data_analyst_role;
-- GRANT SELECT ON metadata_sources, metadata_connections, metadata_pipelines TO data_analyst_role;
-- GRANT SELECT ON execution_log, execution_details, performance_metrics TO data_analyst_role;

-- ============================================================================
-- END OF SCHEMA CREATION SCRIPT
-- ============================================================================
