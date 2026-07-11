# ============================================================================
# BASE SERVICE LAYER
# Abstract base classes and common service functionality
# ============================================================================

from abc import ABC, abstractmethod
from typing import Any, Optional, List, Dict
from datetime import datetime
import logging
from enum import Enum


# ============================================================================
# LOGGER CONFIGURATION
# ============================================================================

class ServiceLogger:
    """Centralized logging for services"""
    
    def __init__(self, service_name: str):
        self.logger = logging.getLogger(service_name)
        self.service_name = service_name
    
    def info(self, message: str, **kwargs):
        self.logger.info(f"[{self.service_name}] {message}", extra=kwargs)
    
    def error(self, message: str, exception: Optional[Exception] = None, **kwargs):
        if exception:
            self.logger.error(f"[{self.service_name}] {message}", exc_info=exception, extra=kwargs)
        else:
            self.logger.error(f"[{self.service_name}] {message}", extra=kwargs)
    
    def warning(self, message: str, **kwargs):
        self.logger.warning(f"[{self.service_name}] {message}", extra=kwargs)
    
    def debug(self, message: str, **kwargs):
        self.logger.debug(f"[{self.service_name}] {message}", extra=kwargs)


# ============================================================================
# EXCEPTION CLASSES
# ============================================================================

class FrameworkException(Exception):
    """Base exception for framework"""
    def __init__(self, message: str, error_code: str = "UNKNOWN_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class ConnectionException(FrameworkException):
    """Connection-related errors"""
    def __init__(self, message: str):
        super().__init__(message, "CONNECTION_ERROR")


class ValidationException(FrameworkException):
    """Validation-related errors"""
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")


class ExtractionException(FrameworkException):
    """Extraction-related errors"""
    def __init__(self, message: str):
        super().__init__(message, "EXTRACTION_ERROR")


class ConfigurationException(FrameworkException):
    """Configuration-related errors"""
    def __init__(self, message: str):
        super().__init__(message, "CONFIGURATION_ERROR")


class StorageException(FrameworkException):
    """Storage operation errors"""
    def __init__(self, message: str):
        super().__init__(message, "STORAGE_ERROR")


# ============================================================================
# RETRY STRATEGY
# ============================================================================

class RetryStrategy:
    """Exponential backoff retry strategy"""
    
    def __init__(
        self,
        max_attempts: int = 5,
        initial_delay_seconds: float = 1.0,
        max_delay_seconds: float = 60.0,
        backoff_multiplier: float = 2.0,
        jitter: bool = True
    ):
        self.max_attempts = max_attempts
        self.initial_delay_seconds = initial_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter
    
    def calculate_delay(self, attempt_number: int) -> float:
        """Calculate delay for given attempt"""
        import random
        import math
        
        delay = min(
            self.initial_delay_seconds * (self.backoff_multiplier ** attempt_number),
            self.max_delay_seconds
        )
        
        if self.jitter:
            delay = delay * (0.5 + random.random())
        
        return delay
    
    def should_retry(self, attempt_number: int, exception: Exception) -> bool:
        """Determine if should retry based on exception type"""
        # Don't retry on validation or configuration errors
        if isinstance(exception, (ValidationException, ConfigurationException)):
            return False
        # Retry on connection and extraction errors
        return attempt_number < self.max_attempts


# ============================================================================
# BASE SERVICE CLASS
# ============================================================================

class BaseService(ABC):
    """Abstract base class for all services"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = ServiceLogger(service_name)
        self.created_at = datetime.utcnow()
    
    @abstractmethod
    def validate_input(self, **kwargs) -> bool:
        """Validate input parameters"""
        pass
    
    def log_operation_start(self, operation: str, **context):
        """Log start of operation"""
        self.logger.info(f"Starting operation: {operation}", extra=context)
    
    def log_operation_end(self, operation: str, duration_ms: int, **context):
        """Log end of operation"""
        self.logger.info(
            f"Completed operation: {operation} (duration: {duration_ms}ms)",
            extra=context
        )
    
    def log_error_operation(self, operation: str, exception: Exception, **context):
        """Log operation error"""
        self.logger.error(
            f"Failed operation: {operation}",
            exception=exception,
            extra=context
        )


# ============================================================================
# METADATA SERVICE INTERFACE
# ============================================================================

class IMetadataService(BaseService):
    """Interface for metadata operations"""
    
    @abstractmethod
    def get_pipeline_by_id(self, pipeline_id: int) -> Any:
        """Retrieve pipeline configuration by ID"""
        pass
    
    @abstractmethod
    def get_pipeline_by_name(self, pipeline_name: str) -> Any:
        """Retrieve pipeline configuration by name"""
        pass
    
    @abstractmethod
    def get_source(self, source_id: int) -> Any:
        """Retrieve source system definition"""
        pass
    
    @abstractmethod
    def get_connection(self, connection_id: int) -> Any:
        """Retrieve connection details"""
        pass
    
    @abstractmethod
    def get_source_table(self, table_id: int) -> Any:
        """Retrieve source table metadata"""
        pass
    
    @abstractmethod
    def get_fields(self, table_id: int) -> List[Any]:
        """Retrieve field mappings for table"""
        pass
    
    @abstractmethod
    def get_validations(self, table_id: int) -> List[Any]:
        """Retrieve validation rules for table"""
        pass
    
    @abstractmethod
    def get_transformations(self, table_id: int) -> List[Any]:
        """Retrieve transformations for table"""
        pass
    
    @abstractmethod
    def update_pipeline_checkpoint(self, pipeline_id: int, checkpoint_data: Dict) -> bool:
        """Update checkpoint for resumable extraction"""
        pass


# ============================================================================
# CONNECTOR SERVICE INTERFACE
# ============================================================================

class IConnector(ABC):
    """Interface for data source connectors"""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to source"""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate connection is working"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute query against source"""
        pass
    
    @abstractmethod
    def get_table_metadata(self, table_name: str) -> Dict:
        """Get metadata for table"""
        pass
    
    @abstractmethod
    def list_tables(self) -> List[str]:
        """List available tables"""
        pass


class IConnectorFactory(ABC):
    """Factory interface for creating connectors"""
    
    @abstractmethod
    def create_connector(self, source_type: str, connection_config: Dict) -> IConnector:
        """Create appropriate connector based on source type"""
        pass


# ============================================================================
# TRANSFORMATION SERVICE INTERFACE
# ============================================================================

class ITransformationService(BaseService):
    """Interface for data transformations"""
    
    @abstractmethod
    def apply_type_conversions(self, dataframe: Any, field_mappings: List[Dict]) -> Any:
        """Convert data types"""
        pass
    
    @abstractmethod
    def apply_standardization(self, dataframe: Any, rules: List[Dict]) -> Any:
        """Standardize data format"""
        pass
    
    @abstractmethod
    def apply_masking(self, dataframe: Any, masking_config: List[Dict]) -> Any:
        """Apply PII masking"""
        pass
    
    @abstractmethod
    def apply_deduplication(self, dataframe: Any, key_columns: List[str]) -> Any:
        """Remove duplicate records"""
        pass
    
    @abstractmethod
    def apply_enrichment(self, dataframe: Any, dimension_data: Dict) -> Any:
        """Enrich data with dimension tables"""
        pass


# ============================================================================
# VALIDATION SERVICE INTERFACE
# ============================================================================

class IValidationService(BaseService):
    """Interface for data quality validation"""
    
    @abstractmethod
    def validate_configuration(self, config: Dict) -> tuple[bool, List[str]]:
        """Validate pipeline configuration"""
        pass
    
    @abstractmethod
    def validate_connection(self, connection_config: Dict) -> tuple[bool, str]:
        """Validate connection parameters"""
        pass
    
    @abstractmethod
    def validate_data_quality(self, dataframe: Any, rules: List[Dict]) -> Dict:
        """Apply data quality rules"""
        pass
    
    @abstractmethod
    def validate_schema(self, dataframe: Any, expected_schema: Dict) -> tuple[bool, List[str]]:
        """Validate data schema matches expected"""
        pass


# ============================================================================
# STORAGE SERVICE INTERFACE
# ============================================================================

class IStorageService(BaseService):
    """Interface for storage operations"""
    
    @abstractmethod
    def upload_to_s3(
        self,
        dataframe: Any,
        s3_path: str,
        partition_columns: Optional[List[str]] = None,
        format: str = "PARQUET"
    ) -> Dict:
        """Upload data to S3"""
        pass
    
    @abstractmethod
    def create_partition(self, s3_path: str, partition_spec: Dict) -> bool:
        """Create S3 partition"""
        pass
    
    @abstractmethod
    def write_manifest(self, manifest_data: Dict, manifest_path: str) -> bool:
        """Write manifest file"""
        pass


# ============================================================================
# AUDIT SERVICE INTERFACE
# ============================================================================

class IAuditService(BaseService):
    """Interface for audit logging"""
    
    @abstractmethod
    def log_execution_start(self, execution_id: int, pipeline_id: int) -> bool:
        """Log execution start"""
        pass
    
    @abstractmethod
    def log_execution_end(
        self,
        execution_id: int,
        status: str,
        metrics: Dict
    ) -> bool:
        """Log execution completion"""
        pass
    
    @abstractmethod
    def log_error(
        self,
        execution_id: int,
        error_type: str,
        error_message: str,
        stack_trace: Optional[str] = None
    ) -> bool:
        """Log execution error"""
        pass
    
    @abstractmethod
    def log_data_quality(
        self,
        execution_id: int,
        quality_results: Dict
    ) -> bool:
        """Log data quality results"""
        pass


# ============================================================================
# CIRCUIT BREAKER PATTERN
# ============================================================================

class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Circuit breaker for fault tolerance"""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_seconds: int = 300,
        expected_exception: type = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
            else:
                raise Exception(f"Circuit breaker is OPEN for {self.__class__.__name__}")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        self.failure_count = 0
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.CLOSED
            self.success_count = 0
    
    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
    
    def _should_attempt_reset(self) -> bool:
        if not self.last_failure_time:
            return True
        elapsed_seconds = (datetime.utcnow() - self.last_failure_time).total_seconds()
        return elapsed_seconds >= self.recovery_timeout_seconds


# ============================================================================
# END OF BASE SERVICES
# ============================================================================
