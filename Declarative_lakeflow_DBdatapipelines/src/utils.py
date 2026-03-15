"""
Utility functions and helpers for data pipelines
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineConfig:
    """Pipeline configuration manager."""
    
    def __init__(self, env: str = "development"):
        """Initialize configuration."""
        self.env = env
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Load environment-specific configuration."""
        configs = {
            "development": {
                "catalog": "main",
                "bronze_schema": "bronze_ecommerce",
                "silver_schema": "silver_ecommerce",
                "gold_schema": "gold_ecommerce",
                "data_path": "/dbfs/data",
                "refresh_interval": "1 hour",
                "log_level": "INFO"
            },
            "production": {
                "catalog": "main",
                "bronze_schema": "bronze_ecommerce_prod",
                "silver_schema": "silver_ecommerce_prod",
                "gold_schema": "gold_ecommerce_prod",
                "data_path": "/mnt/data/production",
                "refresh_interval": "30 minutes",
                "log_level": "WARNING"
            }
        }
        return configs.get(self.env, configs["development"])
    
    def get(self, key: str, default=None):
        """Get configuration value."""
        return self.config.get(key, default)
    
    def to_dict(self) -> Dict:
        """Get config as dictionary."""
        return self.config.copy()


class DataQualityValidator:
    """Data quality validation utilities."""
    
    @staticmethod
    def validate_record_count(df, min_count: int = 1) -> bool:
        """Validate minimum record count."""
        count = df.count()
        is_valid = count >= min_count
        logger.info(f"Record count validation: {count} records (min: {min_count}) - {'PASS' if is_valid else 'FAIL'}")
        return is_valid
    
    @staticmethod
    def validate_null_count(df, column: str, max_nulls: float = 0.05) -> bool:
        """Validate null percentage."""
        from pyspark.sql.functions import isnan, isnull, when, count as spark_count
        
        total_count = df.count()
        null_count = df.select(
            spark_count(when(isnull(df[column]), 1)).
            otherwise(0)
        ).collect()[0][0]
        
        null_percentage = null_count / total_count if total_count > 0 else 0
        is_valid = null_percentage <= max_nulls
        
        logger.info(f"Null validation for {column}: {null_percentage:.2%} nulls (max: {max_nulls:.2%}) - {'PASS' if is_valid else 'FAIL'}")
        return is_valid
    
    @staticmethod
    def validate_unique_count(df, column: str, expected_count: Optional[int] = None) -> bool:
        """Validate unique value count."""
        unique_count = df.select(column).distinct().count()
        
        if expected_count:
            is_valid = unique_count >= expected_count
            logger.info(f"Unique count validation for {column}: {unique_count} unique values (expected: {expected_count}) - {'PASS' if is_valid else 'FAIL'}")
            return is_valid
        
        logger.info(f"Unique count for {column}: {unique_count}")
        return True
    
    @staticmethod
    def validate_column_exists(df, columns: List[str]) -> bool:
        """Validate required columns exist."""
        existing_columns = set(df.columns)
        required_columns = set(columns)
        missing = required_columns - existing_columns
        
        is_valid = len(missing) == 0
        if missing:
            logger.warning(f"Missing columns: {missing}")
        else:
            logger.info(f"All required columns present: {columns}")
        
        return is_valid


class PipelineMetrics:
    """Pipeline metrics and statistics."""
    
    def __init__(self):
        """Initialize metrics collector."""
        self.metrics = {}
        self.start_time = datetime.now()
    
    def add_table_metric(self, table_name: str, record_count: int, 
                        rows_processed: int = 0, errors: int = 0):
        """Record metrics for a table."""
        self.metrics[table_name] = {
            "record_count": record_count,
            "rows_processed": rows_processed,
            "errors": errors,
            "timestamp": datetime.now().isoformat()
        }
        logger.info(f"Metrics for {table_name}: {record_count} records, {rows_processed} processed, {errors} errors")
    
    def get_duration(self) -> str:
        """Get pipeline duration."""
        duration = datetime.now() - self.start_time
        return str(duration)
    
    def get_summary(self) -> Dict:
        """Get metrics summary."""
        total_records = sum(m.get("record_count", 0) for m in self.metrics.values())
        total_errors = sum(m.get("errors", 0) for m in self.metrics.values())
        
        return {
            "total_tables": len(self.metrics),
            "total_records": total_records,
            "total_errors": total_errors,
            "duration": self.get_duration(),
            "tables": self.metrics
        }
    
    def log_summary(self):
        """Log metrics summary."""
        summary = self.get_summary()
        logger.info("=" * 60)
        logger.info("PIPELINE METRICS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Duration: {summary['duration']}")
        logger.info(f"Total Tables: {summary['total_tables']}")
        logger.info(f"Total Records: {summary['total_records']:,}")
        logger.info(f"Total Errors: {summary['total_errors']}")
        logger.info("=" * 60)


class SchemaValidator:
    """Schema validation utilities."""
    
    @staticmethod
    def validate_schema(df, expected_schema: Dict[str, str]) -> bool:
        """Validate DataFrame schema against expected schema."""
        actual_types = {field.name: str(field.dataType) for field in df.schema}
        
        all_valid = True
        for column, expected_type in expected_schema.items():
            if column not in actual_types:
                logger.error(f"Column '{column}' not found in schema")
                all_valid = False
            elif expected_type.lower() not in actual_types[column].lower():
                logger.warning(f"Column '{column}' has type {actual_types[column]}, expected {expected_type}")
        
        return all_valid


class LoggerConfig:
    """Logging configuration helper."""
    
    @staticmethod
    def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
        """Setup logger with standard configuration."""
        logger = logging.getLogger(name)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        logger.setLevel(getattr(logging, level.upper()))
        return logger


if __name__ == "__main__":
    config = PipelineConfig("development")
    print("Pipeline Configuration:")
    print(json.dumps(config.to_dict(), indent=2))
