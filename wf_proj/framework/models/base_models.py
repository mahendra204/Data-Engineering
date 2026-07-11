# ============================================================================
# BASE MODELS
# Data objects representing core concepts in the framework
# ============================================================================

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
import json


# ============================================================================
# ENUMERATIONS
# ============================================================================

class SourceType(Enum):
    """Supported source system types"""
    SQLSERVER = "SQLSERVER"
    ORACLE = "ORACLE"
    POSTGRESQL = "POSTGRESQL"
    MONGODB = "MONGODB"
    FILE_CSV = "FILE_CSV"
    FILE_EXCEL = "FILE_EXCEL"
    FILE_JSON = "FILE_JSON"
    REST_API = "REST_API"
    SFTP = "SFTP"


class ExtractionType(Enum):
    """Types of data extraction patterns"""
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    CDC = "CDC"
    API_PAGINATED = "API_PAGINATED"


class PipelineStatus(Enum):
    """Pipeline execution status"""
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    QUARANTINED = "QUARANTINED"


class ValidationStatus(Enum):
    """Data validation status"""
    PASSED = "PASSED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


class TargetLayer(Enum):
    """S3 data lake layers"""
    RAW = "RAW"
    CURATED = "CURATED"
    ARCHIVE = "ARCHIVE"


# ============================================================================
# SOURCE SYSTEM MODELS
# ============================================================================

@dataclass
class Source:
    """Represents a source system"""
    source_id: int
    source_name: str
    source_type: SourceType
    description: Optional[str] = None
    status: str = "ACTIVE"
    owner_team: Optional[str] = None
    documentation_url: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Connection:
    """Represents a database/service connection"""
    connection_id: int
    source_id: int
    connection_name: str
    environment: str  # DEV, UAT, PROD
    host: str
    port: Optional[int] = None
    database_name: Optional[str] = None
    username: Optional[str] = None
    password_encrypted: Optional[str] = None
    connection_timeout: int = 30
    pool_size: int = 10
    additional_params: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SourceTable:
    """Represents a table/object in source system"""
    table_id: int
    connection_id: int
    source_table_name: str
    table_type: str  # TABLE, VIEW, STORED_PROCEDURE
    display_name: Optional[str] = None
    description: Optional[str] = None
    estimated_row_count: Optional[int] = None
    primary_key_columns: Optional[List[str]] = None
    is_incremental: bool = False
    cdc_enabled: bool = False
    cdc_column: Optional[str] = None
    extraction_frequency: str = "DAILY"


@dataclass
class Field:
    """Represents a field mapping"""
    field_id: int
    table_id: int
    source_field_name: str
    target_field_name: str
    source_data_type: Optional[str] = None
    target_data_type: Optional[str] = None
    is_key_field: bool = False
    is_required: bool = False
    is_masked: bool = False
    masking_type: Optional[str] = None
    transformation_expression: Optional[str] = None
    validation_rule: Optional[str] = None
    default_value: Optional[str] = None


@dataclass
class Validation:
    """Represents a data quality validation rule"""
    validation_id: int
    table_id: int
    validation_name: str
    validation_type: str  # NULL_CHECK, RANGE_CHECK, REGEX, etc.
    target_field: Optional[str] = None
    rule_expression: Optional[str] = None
    threshold: float = 5.0  # Acceptable percentage of violations
    action_on_failure: str = "REJECT"  # REJECT, QUARANTINE, FLAG
    severity: str = "HIGH"  # HIGH, MEDIUM, LOW
    is_enabled: bool = True


@dataclass
class Transformation:
    """Represents a data transformation step"""
    transformation_id: int
    table_id: int
    transformation_name: str
    transformation_type: str  # TYPE_CONVERT, JOIN, AGGREGATE, etc.
    sequence_number: int
    transformation_sql: Optional[str] = None
    transformation_python: Optional[str] = None
    description: Optional[str] = None
    is_enabled: bool = True


# ============================================================================
# PIPELINE MODELS
# ============================================================================

@dataclass
class Pipeline:
    """Represents an extraction pipeline configuration"""
    pipeline_id: int
    pipeline_name: str
    source_id: int
    table_id: int
    connection_id: int
    extraction_type: ExtractionType
    extraction_query: Optional[str] = None
    extraction_method: str = "QUERY"
    target_s3_prefix: str = ""
    target_layer: TargetLayer = TargetLayer.RAW
    partition_columns: Optional[List[str]] = None
    file_format: str = "PARQUET"
    compression_type: str = "SNAPPY"
    batch_size: int = 100000
    chunk_size_mb: int = 256
    is_incremental: bool = False
    last_extraction_timestamp: Optional[datetime] = None
    cdc_last_scn: Optional[str] = None
    data_retention_days: int = 90
    is_enabled: bool = True
    description: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Related objects (lazy-loaded)
    fields: Optional[List[Field]] = None
    validations: Optional[List[Validation]] = None
    transformations: Optional[List[Transformation]] = None


@dataclass
class PipelineState:
    """Represents the runtime state of a pipeline"""
    state_id: int
    pipeline_id: int
    is_paused: bool = False
    is_locked: bool = False
    lock_reason: Optional[str] = None
    last_successful_execution_id: Optional[int] = None
    last_successful_run_time: Optional[datetime] = None
    consecutive_failures: int = 0


# ============================================================================
# EXECUTION MODELS
# ============================================================================

@dataclass
class ExecutionRequest:
    """Request to execute a pipeline"""
    pipeline_id: int
    execution_type: str = "MANUAL"  # SCHEDULED, MANUAL, API_TRIGGER
    triggered_by: str = "SYSTEM"
    force_full_reload: bool = False
    custom_parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionMetrics:
    """Performance metrics for an execution"""
    connection_time_ms: int = 0
    query_execution_time_ms: int = 0
    data_transformation_time_ms: int = 0
    s3_upload_time_ms: int = 0
    total_execution_time_ms: int = 0
    records_per_second: float = 0.0
    memory_used_mb: float = 0.0
    average_batch_time_ms: int = 0


@dataclass
class ExecutionLog:
    """Tracks execution of a pipeline"""
    execution_id: int
    pipeline_id: int
    status: PipelineStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    source_system: Optional[str] = None
    source_table: Optional[str] = None
    target_s3_location: Optional[str] = None
    execution_status_message: Optional[str] = None
    triggered_by: str = "SYSTEM"
    retry_count: int = 0
    parent_execution_id: Optional[int] = None
    data_quality_score: Optional[float] = None
    metrics: Optional[ExecutionMetrics] = None
    duration_seconds: Optional[int] = None


@dataclass
class ExecutionDetail:
    """Details of a specific step in execution"""
    detail_id: int
    execution_id: int
    step_number: int
    step_name: str  # VALIDATE_CONFIG, CONNECT, EXTRACT, TRANSFORM, STORE
    step_status: str  # SUCCESS, FAILED, SKIPPED
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    records_processed: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0


@dataclass
class ErrorDetail:
    """Details of an error during execution"""
    error_id: int
    execution_id: int
    error_type: str
    error_message: str
    error_stack_trace: Optional[str] = None
    error_context: Optional[str] = None
    severity: str = "ERROR"  # CRITICAL, ERROR, WARNING, INFO
    is_recoverable: bool = False
    recovery_action: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DataQualityIssue:
    """Represents a data quality issue"""
    quality_log_id: int
    execution_id: int
    validation_name: str
    field_name: Optional[str] = None
    violation_count: int = 0
    violation_percentage: float = 0.0
    violation_samples: Optional[str] = None
    quality_status: ValidationStatus = ValidationStatus.PASSED


@dataclass
class DataLineage:
    """Tracks data lineage from source to target"""
    lineage_id: int
    execution_id: int
    source_system: str
    source_table: str
    source_query_hash: Optional[str] = None
    target_s3_location: str = ""
    transformation_applied: Optional[List[str]] = None
    data_version: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Checkpoint:
    """Tracks extraction checkpoints for resumable extractions"""
    checkpoint_id: int
    pipeline_id: int
    checkpoint_type: str  # FULL_LOAD, INCREMENTAL, CDC
    last_processed_offset: Optional[str] = None
    last_processed_timestamp: Optional[datetime] = None
    last_processed_scn: Optional[str] = None
    checkpoint_data: Dict[str, Any] = field(default_factory=dict)
    updated_at: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# S3 STORAGE MODELS
# ============================================================================

@dataclass
class StorageLocation:
    """Represents an S3 storage location"""
    bucket_name: str
    layer: TargetLayer
    source_system: str
    table_name: str
    load_date: str  # YYYY-MM-DD
    batch_id: Optional[str] = None
    
    def get_s3_path(self) -> str:
        """Generate S3 path from components"""
        path = f"s3://{self.bucket_name}/{self.layer.value}/{self.source_system}/{self.table_name}/{self.load_date}"
        if self.batch_id:
            path += f"/{self.batch_id}"
        return path


@dataclass
class StorageManifest:
    """Manifest file for stored data asset"""
    asset_name: str
    source_system: str
    source_table: str
    extraction_timestamp: datetime
    record_count: int
    file_count: int
    total_size_mb: float
    data_version: str
    s3_location: str
    checksums: Dict[str, str] = field(default_factory=dict)
    
    def to_json(self) -> str:
        """Convert manifest to JSON"""
        return json.dumps({
            "asset_name": self.asset_name,
            "source_system": self.source_system,
            "source_table": self.source_table,
            "extraction_timestamp": self.extraction_timestamp.isoformat(),
            "record_count": self.record_count,
            "file_count": self.file_count,
            "total_size_mb": self.total_size_mb,
            "data_version": self.data_version,
            "s3_location": self.s3_location,
            "checksums": self.checksums
        })


@dataclass
class DataAsset:
    """Represents a data asset in S3"""
    asset_id: int
    execution_id: int
    asset_name: str
    source_system: str
    source_table: str
    s3_location: str
    layer: TargetLayer
    record_count: int
    file_count: int
    total_size_mb: float
    data_version: str
    lifecycle_stage: str = "ACTIVE"  # ACTIVE, ARCHIVED, DELETED
    created_at: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# RESPONSE MODELS
# ============================================================================

@dataclass
class ExecutionResponse:
    """Response from pipeline execution"""
    execution_id: int
    status: PipelineStatus
    total_records: int
    successful_records: int
    failed_records: int
    error_message: Optional[str] = None
    target_s3_location: Optional[str] = None
    metrics: Optional[ExecutionMetrics] = None
    quality_score: Optional[float] = None


@dataclass
class FrameworkResponse:
    """Generic framework response"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    errors: Optional[List[str]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# END OF MODELS
# ============================================================================
