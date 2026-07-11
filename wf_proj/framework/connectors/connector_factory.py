# ============================================================================
# CONNECTOR IMPLEMENTATIONS
# Concrete implementations for different data sources
# ============================================================================

from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
import json

from ..services.base_service import IConnector, ServiceLogger, ConnectionException


# ============================================================================
# SQL-BASED CONNECTOR (Base for SQL Server, Oracle, PostgreSQL)
# ============================================================================

class SQLConnector(IConnector, ABC):
    """Base class for SQL-based connectors"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.connection = None
        self.logger = ServiceLogger(self.__class__.__name__)
    
    @abstractmethod
    def _create_connection(self):
        """Create database connection - to be implemented by subclasses"""
        pass
    
    def connect(self) -> bool:
        """Establish connection to database"""
        try:
            self.logger.info("Attempting database connection")
            self.connection = self._create_connection()
            self.logger.info("Database connection successful")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}", exception=e)
            raise ConnectionException(f"Failed to connect: {str(e)}")
    
    def validate_connection(self) -> bool:
        """Test connection with validation query"""
        try:
            if not self.connection:
                raise ConnectionException("No active connection")
            
            validation_query = self.config.get('validation_query', 'SELECT 1')
            cursor = self.connection.cursor()
            cursor.execute(validation_query)
            cursor.close()
            
            self.logger.info("Connection validation successful")
            return True
        except Exception as e:
            self.logger.error(f"Connection validation failed: {str(e)}", exception=e)
            return False
    
    def disconnect(self) -> None:
        """Close database connection"""
        try:
            if self.connection:
                self.connection.close()
                self.logger.info("Connection closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing connection: {str(e)}", exception=e)
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        try:
            if not self.connection:
                raise ConnectionException("No active connection")
            
            self.logger.info(f"Executing query with {len(params) if params else 0} parameters")
            
            # Using parameterized queries to prevent SQL injection
            df = pd.read_sql(query, self.connection, params=params)
            
            self.logger.info(f"Query executed successfully. Retrieved {len(df)} records")
            return df
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}", exception=e)
            raise ConnectionException(f"Query execution failed: {str(e)}")
    
    def get_table_metadata(self, table_name: str) -> Dict:
        """Get column information for table"""
        try:
            query = self._get_table_metadata_query(table_name)
            df = self.execute_query(query)
            
            metadata = {
                'table_name': table_name,
                'columns': df.to_dict('records') if len(df) > 0 else [],
                'retrieved_at': datetime.utcnow().isoformat()
            }
            return metadata
        except Exception as e:
            self.logger.error(f"Failed to get table metadata: {str(e)}", exception=e)
            raise
    
    @abstractmethod
    def _get_table_metadata_query(self, table_name: str) -> str:
        """Get database-specific query for table metadata"""
        pass
    
    def list_tables(self) -> List[str]:
        """List all available tables"""
        try:
            query = self._get_list_tables_query()
            df = self.execute_query(query)
            tables = df.iloc[:, 0].tolist() if len(df) > 0 else []
            self.logger.info(f"Retrieved {len(tables)} table names")
            return tables
        except Exception as e:
            self.logger.error(f"Failed to list tables: {str(e)}", exception=e)
            raise
    
    @abstractmethod
    def _get_list_tables_query(self) -> str:
        """Get database-specific query for listing tables"""
        pass


# ============================================================================
# SQL SERVER CONNECTOR
# ============================================================================

class SQLServerConnector(SQLConnector):
    """Connector for SQL Server databases"""
    
    def _create_connection(self):
        """Create SQL Server connection using PyODBC"""
        try:
            import pyodbc
            
            connection_string = (
                f"Driver={{ODBC Driver 17 for SQL Server}};"
                f"Server={self.config['host']},{self.config.get('port', 1433)};"
                f"Database={self.config['database_name']};"
                f"UID={self.config['username']};"
                f"PWD={self.config['password_encrypted']};"
                f"Connection Timeout={self.config.get('connection_timeout', 30)};"
            )
            
            connection = pyodbc.connect(connection_string)
            return connection
        except ImportError:
            raise ConnectionException("PyODBC library not installed")
        except Exception as e:
            raise ConnectionException(f"SQL Server connection failed: {str(e)}")
    
    def _get_table_metadata_query(self, table_name: str) -> str:
        """Get SQL Server table metadata"""
        return f"""
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            CHARACTER_MAXIMUM_LENGTH as max_length,
            IS_NULLABLE as is_nullable
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
    
    def _get_list_tables_query(self) -> str:
        """Get SQL Server list of tables"""
        return """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """


# ============================================================================
# ORACLE CONNECTOR
# ============================================================================

class OracleConnector(SQLConnector):
    """Connector for Oracle databases"""
    
    def _create_connection(self):
        """Create Oracle connection using cx_Oracle"""
        try:
            import cx_Oracle
            
            dsn = cx_Oracle.makedsn(
                self.config['host'],
                self.config.get('port', 1521),
                service_name=self.config['database_name']
            )
            
            connection = cx_Oracle.connect(
                user=self.config['username'],
                password=self.config['password_encrypted'],
                dsn=dsn,
                threaded=True
            )
            
            return connection
        except ImportError:
            raise ConnectionException("cx_Oracle library not installed")
        except Exception as e:
            raise ConnectionException(f"Oracle connection failed: {str(e)}")
    
    def _get_table_metadata_query(self, table_name: str) -> str:
        """Get Oracle table metadata"""
        return f"""
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            DATA_LENGTH as max_length,
            NULLABLE as is_nullable
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = UPPER('{table_name}')
        ORDER BY COLUMN_ID
        """
    
    def _get_list_tables_query(self) -> str:
        """Get Oracle list of tables"""
        return "SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME"


# ============================================================================
# MONGODB CONNECTOR
# ============================================================================

class MongoConnector(IConnector):
    """Connector for MongoDB databases"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.client = None
        self.database = None
        self.logger = ServiceLogger("MongoConnector")
    
    def connect(self) -> bool:
        """Establish connection to MongoDB"""
        try:
            import pymongo
            
            connection_string = (
                f"mongodb://{self.config['username']}:{self.config['password_encrypted']}"
                f"@{self.config['host']}:{self.config.get('port', 27017)}"
                f"/{self.config.get('database_name', 'admin')}"
            )
            
            self.client = pymongo.MongoClient(
                connection_string,
                serverSelectionTimeoutMS=self.config.get('connection_timeout', 30) * 1000
            )
            self.database = self.client[self.config['database_name']]
            
            self.logger.info("MongoDB connection successful")
            return True
        except ImportError:
            raise ConnectionException("pymongo library not installed")
        except Exception as e:
            self.logger.error(f"MongoDB connection failed: {str(e)}", exception=e)
            raise ConnectionException(f"MongoDB connection failed: {str(e)}")
    
    def validate_connection(self) -> bool:
        """Test MongoDB connection"""
        try:
            self.client.admin.command('ping')
            self.logger.info("MongoDB validation successful")
            return True
        except Exception as e:
            self.logger.error(f"MongoDB validation failed: {str(e)}", exception=e)
            return False
    
    def disconnect(self) -> None:
        """Close MongoDB connection"""
        try:
            if self.client:
                self.client.close()
                self.logger.info("MongoDB connection closed")
        except Exception as e:
            self.logger.error(f"Error closing MongoDB connection: {str(e)}", exception=e)
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute MongoDB query (collection name in query)"""
        try:
            if not self.database:
                raise ConnectionException("No active MongoDB connection")
            
            collection_name = query.strip()
            collection = self.database[collection_name]
            
            filter_criteria = params.get('filter', {}) if params else {}
            projection = params.get('projection', None) if params else None
            
            cursor = collection.find(filter_criteria, projection)
            data = list(cursor)
            
            # Convert MongoDB ObjectId to string for JSON serialization
            for record in data:
                if '_id' in record:
                    record['_id'] = str(record['_id'])
            
            df = pd.DataFrame(data)
            self.logger.info(f"Retrieved {len(df)} documents from collection {collection_name}")
            return df
        except Exception as e:
            self.logger.error(f"MongoDB query failed: {str(e)}", exception=e)
            raise ConnectionException(f"MongoDB query failed: {str(e)}")
    
    def get_table_metadata(self, table_name: str) -> Dict:
        """Get MongoDB collection metadata"""
        try:
            collection = self.database[table_name]
            
            # Sample first document to infer schema
            sample_doc = collection.find_one()
            
            metadata = {
                'collection_name': table_name,
                'document_count': collection.count_documents({}),
                'sample_document': {k: str(type(v).__name__) for k, v in sample_doc.items()} if sample_doc else {},
                'indexes': list(collection.list_indexes()),
                'retrieved_at': datetime.utcnow().isoformat()
            }
            return metadata
        except Exception as e:
            self.logger.error(f"Failed to get collection metadata: {str(e)}", exception=e)
            raise
    
    def list_tables(self) -> List[str]:
        """List MongoDB collections"""
        try:
            collections = self.database.list_collection_names()
            self.logger.info(f"Retrieved {len(collections)} collections")
            return collections
        except Exception as e:
            self.logger.error(f"Failed to list collections: {str(e)}", exception=e)
            raise


# ============================================================================
# CSV FILE CONNECTOR
# ============================================================================

class CSVConnector(IConnector):
    """Connector for CSV files"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.logger = ServiceLogger("CSVConnector")
        self.connected = False
    
    def connect(self) -> bool:
        """Validate file access"""
        try:
            file_path = self.config.get('file_path')
            if not file_path:
                raise ConnectionException("file_path not specified in config")
            
            import os
            if not os.path.exists(file_path):
                raise ConnectionException(f"File not found: {file_path}")
            
            self.connected = True
            self.logger.info(f"CSV file accessible: {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"CSV connection failed: {str(e)}", exception=e)
            raise ConnectionException(f"CSV connection failed: {str(e)}")
    
    def validate_connection(self) -> bool:
        """Validate CSV can be read"""
        try:
            self.execute_query("", {"nrows": 1})
            return True
        except:
            return False
    
    def disconnect(self) -> None:
        """Close file connection"""
        self.connected = False
        self.logger.info("CSV connection closed")
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Read CSV file"""
        try:
            file_path = self.config.get('file_path')
            
            read_params = {
                'delimiter': self.config.get('delimiter', ','),
                'encoding': self.config.get('encoding', 'utf-8')
            }
            
            if params:
                read_params.update(params)
            
            df = pd.read_csv(file_path, **read_params)
            self.logger.info(f"Read {len(df)} rows from CSV")
            return df
        except Exception as e:
            self.logger.error(f"CSV read failed: {str(e)}", exception=e)
            raise ConnectionException(f"CSV read failed: {str(e)}")
    
    def get_table_metadata(self, table_name: str) -> Dict:
        """Get CSV metadata"""
        try:
            df = self.execute_query("", {"nrows": 0})
            metadata = {
                'file_name': table_name,
                'columns': list(df.columns),
                'column_types': df.dtypes.to_dict(),
                'retrieved_at': datetime.utcnow().isoformat()
            }
            return metadata
        except Exception as e:
            self.logger.error(f"Failed to get CSV metadata: {str(e)}", exception=e)
            raise
    
    def list_tables(self) -> List[str]:
        """List CSV files in directory"""
        try:
            import os
            file_path = self.config.get('file_path')
            directory = os.path.dirname(file_path)
            csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
            return csv_files
        except Exception as e:
            self.logger.error(f"Failed to list CSV files: {str(e)}", exception=e)
            raise


# ============================================================================
# CONNECTOR FACTORY
# ============================================================================

class ConnectorFactory:
    """Factory for creating appropriate connectors"""
    
    _connectors = {
        'SQLSERVER': SQLServerConnector,
        'ORACLE': OracleConnector,
        'MONGODB': MongoConnector,
        'FILE_CSV': CSVConnector,
        # Additional connectors can be registered here
    }
    
    @staticmethod
    def create_connector(source_type: str, connection_config: Dict) -> IConnector:
        """Create connector based on source type"""
        logger = ServiceLogger("ConnectorFactory")
        
        if source_type not in ConnectorFactory._connectors:
            raise ConnectionException(f"Unsupported source type: {source_type}")
        
        connector_class = ConnectorFactory._connectors[source_type]
        logger.info(f"Creating connector for source type: {source_type}")
        
        return connector_class(connection_config)
    
    @staticmethod
    def register_connector(source_type: str, connector_class: type):
        """Register a custom connector"""
        ConnectorFactory._connectors[source_type] = connector_class


# ============================================================================
# END OF CONNECTORS
# ============================================================================
