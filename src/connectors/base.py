"""
Base connector interface for CDC operations
All database connectors must implement this interface
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Iterator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class OperationType(Enum):
    """CDC operation types"""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SNAPSHOT = "SNAPSHOT"  # Initial data load


@dataclass
class ChangeEvent:
    """
    Represents a single change data capture event
    """
    operation: OperationType
    table: str
    schema: Optional[str]
    timestamp: datetime
    before: Optional[Dict[str, Any]]  # Data before change (for UPDATE/DELETE)
    after: Optional[Dict[str, Any]]   # Data after change (for INSERT/UPDATE)
    primary_key: Dict[str, Any]       # Primary key values
    lsn: Optional[str] = None         # Log Sequence Number (PostgreSQL)
    gtid: Optional[str] = None        # Global Transaction ID (MySQL)
    source_db: Optional[str] = None   # Source database identifier
    
    def __repr__(self) -> str:
        return (
            f"ChangeEvent(op={self.operation.value}, "
            f"table={self.table}, "
            f"pk={self.primary_key}, "
            f"ts={self.timestamp})"
        )


@dataclass
class TableSchema:
    """
    Represents table schema information
    """
    table_name: str
    schema_name: Optional[str]
    columns: List[Dict[str, Any]]  # [{name, type, nullable, default}, ...]
    primary_keys: List[str]
    indexes: List[Dict[str, Any]]
    
    def get_column_type(self, column_name: str) -> Optional[str]:
        """Get data type for a specific column"""
        for col in self.columns:
            if col["name"] == column_name:
                return col["type"]
        return None


class BaseConnector(ABC):
    """
    Abstract base class for database CDC connectors
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize connector with configuration
        
        Args:
            config: Database connection configuration
        """
        self.config = config
        self.connected = False
    
    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the database
        Should set self.connected = True on success
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """
        Close database connection
        Should set self.connected = False
        """
        pass
    
    @abstractmethod
    def setup_cdc(self, tables: List[str]) -> None:
        """
        Set up CDC infrastructure (replication slots, publications, etc.)
        
        Args:
            tables: List of table names to monitor
        """
        pass
    
    @abstractmethod
    def start_streaming(self, start_lsn: Optional[str] = None) -> Iterator[ChangeEvent]:
        """
        Start streaming change events
        
        Args:
            start_lsn: Starting position (LSN for PostgreSQL, GTID for MySQL)
            
        Yields:
            ChangeEvent: Individual change events
        """
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> TableSchema:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema: Schema information
        """
        pass
    
    @abstractmethod
    def apply_change(self, event: ChangeEvent) -> bool:
        """
        Apply a change event to the database
        
        Args:
            event: Change event to apply
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Any:
        """
        Execute a SQL query
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Any: Query results
        """
        pass
    
    @abstractmethod
    def get_current_position(self) -> str:
        """
        Get current replication position (LSN/GTID)
        
        Returns:
            str: Current position identifier
        """
        pass
    
    def get_primary_key_values(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract primary key values from data
        
        Args:
            table: Table name
            data: Row data
            
        Returns:
            Dict with primary key column names and values
        """
        schema = self.get_table_schema(table)
        return {pk: data.get(pk) for pk in schema.primary_keys if pk in data}
    
    def health_check(self) -> bool:
        """
        Check if connection is healthy
        
        Returns:
            bool: True if healthy, False otherwise
        """
        return self.connected
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
        return False


class ConnectorFactory:
    """
    Factory for creating database connectors
    """
    
    _connectors: Dict[str, type] = {}
    
    @classmethod
    def register(cls, db_type: str, connector_class: type):
        """
        Register a connector class for a database type
        
        Args:
            db_type: Database type identifier (e.g., 'postgresql', 'mysql')
            connector_class: Connector class implementing BaseConnector
        """
        cls._connectors[db_type.lower()] = connector_class
    
    @classmethod
    def create(cls, db_type: str, config: Dict[str, Any]) -> BaseConnector:
        """
        Create a connector instance
        
        Args:
            db_type: Database type
            config: Database configuration
            
        Returns:
            BaseConnector: Connector instance
            
        Raises:
            ValueError: If database type is not supported
        """
        connector_class = cls._connectors.get(db_type.lower())
        if not connector_class:
            raise ValueError(
                f"Unsupported database type: {db_type}. "
                f"Available types: {list(cls._connectors.keys())}"
            )
        return connector_class(config)
    
    @classmethod
    def supported_types(cls) -> List[str]:
        """
        Get list of supported database types
        
        Returns:
            List of database type identifiers
        """
        return list(cls._connectors.keys())


# Example usage
if __name__ == "__main__":
    # Example of creating a change event
    event = ChangeEvent(
        operation=OperationType.INSERT,
        table="users",
        schema="public",
        timestamp=datetime.now(),
        before=None,
        after={"id": 1, "name": "John Doe", "email": "john@example.com"},
        primary_key={"id": 1},
        lsn="0/1A2B3C4D"
    )
    
    print(event)
    print(f"\nOperation: {event.operation.value}")
    print(f"Table: {event.table}")
    print(f"Data: {event.after}")