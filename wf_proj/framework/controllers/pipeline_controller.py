# ============================================================================
# PIPELINE CONTROLLER
# Main orchestration logic for extraction pipelines
# ============================================================================

import time
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import hashlib

from ..models.base_models import (
    Pipeline, ExecutionLog, ExecutionRequest, ExecutionResponse, 
    PipelineStatus, ExecutionMetrics, ExecutionDetail, ErrorDetail,
    DataQualityIssue, DataLineage, StorageLocation, ExecutionRequest
)
from ..services.base_service import (
    ServiceLogger, FrameworkException, RetryStrategy, CircuitBreaker
)
from ..connectors.connector_factory import ConnectorFactory


# ============================================================================
# PIPELINE CONTROLLER
# ============================================================================

class PipelineController:
    """Main controller for orchestrating extraction pipelines"""
    
    def __init__(
        self,
        metadata_service,
        storage_service,
        transform_service,
        validation_service,
        audit_service,
        max_retries: int = 5
    ):
        self.metadata_service = metadata_service
        self.storage_service = storage_service
        self.transform_service = transform_service
        self.validation_service = validation_service
        self.audit_service = audit_service
        
        self.logger = ServiceLogger("PipelineController")
        self.max_retries = max_retries
        self.retry_strategy = RetryStrategy(max_attempts=max_retries)
        self.circuit_breaker = CircuitBreaker(failure_threshold=5)
    
    def execute_pipeline(self, execution_request: ExecutionRequest) -> ExecutionResponse:
        """
        Main pipeline execution orchestration
        
        Flow:
        1. Load pipeline configuration
        2. Create execution log entry
        3. Validate configuration
        4. Connect to source
        5. Extract data
        6. Validate data quality
        7. Transform data
        8. Store to S3
        9. Update metadata
        10. Complete execution
        """
        execution_start_time = time.time()
        execution_metrics = ExecutionMetrics()
        execution_id = None
        connector = None
        
        try:
            # ===== STEP 1: LOAD CONFIGURATION =====
            self.logger.info(
                f"Loading pipeline configuration for pipeline_id={execution_request.pipeline_id}",
                extra={"execution_request": execution_request.__dict__}
            )
            
            step_start = time.time()
            pipeline = self.metadata_service.get_pipeline_by_id(execution_request.pipeline_id)
            execution_metrics.connection_time_ms = int((time.time() - step_start) * 1000)
            
            # ===== STEP 2: CREATE EXECUTION LOG =====
            execution_log = ExecutionLog(
                execution_id=0,  # Will be assigned by audit service
                pipeline_id=pipeline.pipeline_id,
                status=PipelineStatus.RUNNING,
                started_at=datetime.utcnow(),
                triggered_by=execution_request.triggered_by
            )
            execution_id = self.audit_service.log_execution_start(execution_log)
            
            # ===== STEP 3: VALIDATE CONFIGURATION =====
            self.logger.info(f"Validating pipeline configuration (execution_id={execution_id})")
            is_valid, validation_errors = self._validate_pipeline_config(pipeline)
            if not is_valid:
                raise FrameworkException(
                    f"Configuration validation failed: {', '.join(validation_errors)}"
                )
            
            # ===== STEP 4: ESTABLISH CONNECTION =====
            self.logger.info(f"Establishing connection to source system (execution_id={execution_id})")
            step_start = time.time()
            
            connector = self._create_connector(pipeline)
            connector.connect()
            connector.validate_connection()
            
            execution_metrics.connection_time_ms = int((time.time() - step_start) * 1000)
            
            # ===== STEP 5: EXTRACT DATA =====
            self.logger.info(f"Extracting data from source (execution_id={execution_id})")
            step_start = time.time()
            
            extraction_query = pipeline.extraction_query
            extracted_data = connector.execute_query(extraction_query)
            
            execution_metrics.query_execution_time_ms = int((time.time() - step_start) * 1000)
            execution_log.total_records = len(extracted_data)
            
            # ===== STEP 6: VALIDATE DATA QUALITY =====
            self.logger.info(f"Validating data quality (execution_id={execution_id})")
            step_start = time.time()
            
            # Get validation rules from metadata
            validations = self.metadata_service.get_validations(pipeline.table_id)
            quality_results = self._validate_data_quality(extracted_data, validations)
            
            execution_log.data_quality_score = quality_results.get('overall_score', 0)
            
            # Check if quality failures require rejection
            if quality_results.get('reject', False):
                raise FrameworkException(
                    f"Data quality checks failed: {quality_results.get('message', 'Unknown')}"
                )
            
            # ===== STEP 7: TRANSFORM DATA =====
            self.logger.info(f"Transforming data (execution_id={execution_id})")
            step_start = time.time()
            
            # Get field mappings and transformations
            fields = self.metadata_service.get_fields(pipeline.table_id)
            transformations = self.metadata_service.get_transformations(pipeline.table_id)
            
            transformed_data = self.transform_service.apply_transformations(
                extracted_data,
                fields,
                transformations
            )
            
            execution_metrics.data_transformation_time_ms = int((time.time() - step_start) * 1000)
            execution_log.successful_records = len(transformed_data)
            
            # ===== STEP 8: STORE TO S3 =====
            self.logger.info(f"Storing data to S3 (execution_id={execution_id})")
            step_start = time.time()
            
            storage_result = self._store_to_s3(
                transformed_data,
                pipeline,
                execution_id
            )
            
            execution_metrics.s3_upload_time_ms = int((time.time() - step_start) * 1000)
            execution_log.target_s3_location = storage_result['s3_location']
            
            # ===== STEP 9: UPDATE METADATA =====
            self.logger.info(f"Updating metadata (execution_id={execution_id})")
            
            if pipeline.is_incremental:
                checkpoint_data = {
                    'last_processed_offset': extraction_query,
                    'last_processed_timestamp': datetime.utcnow().isoformat()
                }
                self.metadata_service.update_pipeline_checkpoint(
                    pipeline.pipeline_id,
                    checkpoint_data
                )
            
            # ===== STEP 10: COMPLETE EXECUTION =====
            execution_log.status = PipelineStatus.SUCCESS
            execution_log.completed_at = datetime.utcnow()
            execution_metrics.total_execution_time_ms = int((time.time() - execution_start_time) * 1000)
            execution_metrics.records_per_second = (
                execution_log.total_records / 
                (execution_metrics.total_execution_time_ms / 1000)
                if execution_metrics.total_execution_time_ms > 0 else 0
            )
            execution_log.metrics = execution_metrics
            
            self.logger.info(
                f"Pipeline execution completed successfully (execution_id={execution_id})",
                extra={
                    "total_records": execution_log.total_records,
                    "duration_ms": execution_metrics.total_execution_time_ms,
                    "s3_location": execution_log.target_s3_location
                }
            )
            
            # Log final execution status
            self.audit_service.log_execution_end(execution_id, execution_log)
            
            return ExecutionResponse(
                execution_id=execution_id,
                status=PipelineStatus.SUCCESS,
                total_records=execution_log.total_records,
                successful_records=execution_log.successful_records,
                failed_records=execution_log.failed_records,
                target_s3_location=execution_log.target_s3_location,
                metrics=execution_metrics,
                quality_score=execution_log.data_quality_score
            )
        
        except Exception as e:
            self.logger.error(
                f"Pipeline execution failed: {str(e)}",
                exception=e,
                extra={"execution_id": execution_id, "pipeline_id": execution_request.pipeline_id}
            )
            
            # Log error to audit service
            if execution_id:
                error_detail = ErrorDetail(
                    error_id=0,
                    execution_id=execution_id,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    severity="CRITICAL"
                )
                self.audit_service.log_error(error_detail)
            
            raise
        
        finally:
            # Clean up resources
            if connector:
                try:
                    connector.disconnect()
                except:
                    pass
    
    # ===== HELPER METHODS =====
    
    def _validate_pipeline_config(self, pipeline: Pipeline) -> tuple[bool, List[str]]:
        """Validate pipeline configuration"""
        errors = []
        
        # Check required fields
        if not pipeline.extraction_query:
            errors.append("extraction_query is required")
        if not pipeline.target_s3_prefix:
            errors.append("target_s3_prefix is required")
        if not pipeline.connection_id:
            errors.append("connection_id is required")
        
        return len(errors) == 0, errors
    
    def _create_connector(self, pipeline: Pipeline):
        """Create appropriate connector based on source type"""
        # Get connection details from metadata
        connection = self.metadata_service.get_connection(pipeline.connection_id)
        source = self.metadata_service.get_source(pipeline.source_id)
        
        # Prepare connection config
        connection_config = {
            'host': connection.host,
            'port': connection.port,
            'database_name': connection.database_name,
            'username': connection.username,
            'password_encrypted': connection.password_encrypted,
            'connection_timeout': connection.connection_timeout,
            'pool_size': connection.pool_size,
            **connection.additional_params
        }
        
        # Create connector using factory
        connector = ConnectorFactory.create_connector(source.source_type.value, connection_config)
        return connector
    
    def _validate_data_quality(self, data, validations) -> Dict:
        """Apply data quality validations"""
        quality_results = {
            'overall_score': 100.0,
            'reject': False,
            'issues': [],
            'message': ''
        }
        
        if not validations or len(data) == 0:
            return quality_results
        
        total_violations = 0
        
        for validation in validations:
            # Apply validation rules
            # This is simplified - full implementation would apply each rule
            violation_count = 0  # Count violations
            violation_pct = (violation_count / len(data) * 100) if len(data) > 0 else 0
            
            if violation_pct > validation.threshold:
                quality_results['issues'].append({
                    'validation': validation.validation_name,
                    'violation_count': violation_count,
                    'violation_percentage': violation_pct
                })
                
                if validation.action_on_failure == 'REJECT' and validation.severity == 'HIGH':
                    quality_results['reject'] = True
                    quality_results['message'] = f"High severity validation failure: {validation.validation_name}"
        
        if quality_results['issues']:
            quality_results['overall_score'] = max(0, 100 - len(quality_results['issues']) * 10)
        
        return quality_results
    
    def _store_to_s3(self, data, pipeline: Pipeline, execution_id: int) -> Dict:
        """Store transformed data to S3"""
        storage_location = StorageLocation(
            bucket_name="data-lake",
            layer=pipeline.target_layer,
            source_system=self.metadata_service.get_source(pipeline.source_id).source_name,
            table_name=self.metadata_service.get_source_table(pipeline.table_id).source_table_name,
            load_date=datetime.utcnow().strftime("%Y-%m-%d"),
            batch_id=f"exec_{execution_id}"
        )
        
        # Upload to S3 using storage service
        result = self.storage_service.upload_to_s3(
            data,
            storage_location.get_s3_path(),
            partition_columns=pipeline.partition_columns,
            format=pipeline.file_format
        )
        
        return result


# ============================================================================
# END OF PIPELINE CONTROLLER
# ============================================================================
