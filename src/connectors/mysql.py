"""
MySQL CDC Connector using binlog replication
Reads from binary logs using mysql-replication library
"""
import mysql.connector
from typing import Dict, Any, List, Optional, Iterator
from datetime import datetime

from .base import (
    BaseConnector, ChangeEvent, TableSchema,
    OperationType, ConnectorFactory
)


class MySQLConnector(BaseConnector):
    """
    MySQL CDC connector using binlog replication
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.conn = None
        self.cursor = None
        self.server_id = config.get("server_id", 100)
    
    def connect(self) -> None:
        """Establish connection to MySQL"""
        try:
            self.conn = mysql.connector.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["name"],
                user=self.config["user"],
                password=self.config["password"],
                autocommit=True
            )
            self.cursor = self.conn.cursor(dictionary=True)
            self.connected = True
            
            print(f"✓ Connected to MySQL: {self.config['host']}:{self.config['port']}")
            
        except Exception as e:
            print(f"✗ Failed to connect to MySQL: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close MySQL connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        
        self.connected = False
        print("Disconnected from MySQL")
    
    def setup_cdc(self, tables: List[str]) -> None:
        """
        Set up CDC infrastructure for MySQL
        Enables binlog if not already enabled
        """
        try:
            # Check if binlog is enabled
            self.cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
            result = self.cursor.fetchone()
            
            if result and result.get("Value") == "ON":
                print("✓ Binary logging is enabled")
            else:
                print("✗ Binary logging is NOT enabled")
                print("  Please enable binlog in MySQL configuration:")
                print("  [mysqld]")
                print("  server-id = 1")
                print("  log-bin = mysql-bin")
                print("  binlog-format = ROW")
                print("  binlog-row-image = FULL")
                return
            
            # Check binlog format
            self.cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
            result = self.cursor.fetchone()
            
            if result and result.get("Value") == "ROW":
                print("✓ Binlog format is ROW")
            else:
                print(f"⚠ Binlog format is {result.get('Value')}, ROW format recommended")
            
            # Verify tables exist
            for table in tables:
                self.cursor.execute(f"SHOW TABLES LIKE '{table}'")
                if not self.cursor.fetchone():
                    print(f"⚠ Table '{table}' does not exist")
                else:
                    print(f"✓ Table '{table}' found")
                    
        except Exception as e:
            print(f"✗ Failed to setup CDC: {e}")
            raise
    
    def start_streaming(self, start_position: Optional[str] = None) -> Iterator[ChangeEvent]:
        """
        Start streaming change events from binlog
        
        Args:
            start_position: Starting binlog position (format: 'filename:position')
            
        Yields:
            ChangeEvent: Change events from binlog
            
        Note: This is a simplified implementation. In production, use
        mysql-replication library (pymysqlreplication) for proper binlog parsing
        """
        try:
            print("Note: Full binlog streaming requires mysql-replication library")
            print("Install with: pip install mysql-replication")
            print("This is a simplified implementation for demonstration")
            
            # In production, you would use:
            # from pymysqlreplication import BinLogStreamReader
            # from pymysqlreplication.row_event import (
            #     DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
            # )
            #
            # stream = BinLogStreamReader(
            #     connection_settings={
            #         'host': self.config['host'],
            #         'port': self.config['port'],
            #         'user': self.config['user'],
            #         'passwd': self.config['password']
            #     },
            #     server_id=self.server_id,
            #     only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            #     resume_stream=True
            # )
            #
            # for binlogevent in stream:
            #     for row in binlogevent.rows:
            #         yield self._parse_binlog_event(binlogevent, row)
            
            # Placeholder for demo
            yield from []
            
        except Exception as e:
            print(f"✗ Streaming error: {e}")
            raise
    
    def _parse_binlog_event(self, event, row) -> ChangeEvent:
        """
        Parse binlog event into ChangeEvent
        
        Args:
            event: Binlog event object
            row: Row data from event
            
        Returns:
            ChangeEvent
        """
        # Determine operation type
        event_type = event.__class__.__name__
        
        if "Write" in event_type:
            operation = OperationType.INSERT
            after = row["values"]
            before = None
        elif "Update" in event_type:
            operation = OperationType.UPDATE
            after = row["after_values"]
            before = row["before_values"]
        elif "Delete" in event_type:
            operation = OperationType.DELETE
            after = None
            before = row["values"]
        else:
            operation = OperationType.SNAPSHOT
            after = row.get("values")
            before = None
        
        # Extract primary key
        primary_key = self._extract_pk_from_row(event.table, before or after)
        
        return ChangeEvent(
            operation=operation,
            table=event.table,
            schema=event.schema,
            timestamp=datetime.fromtimestamp(event.timestamp),
            before=before,
            after=after,
            primary_key=primary_key,
            gtid=str(event.packet.log_pos),
            source_db="mysql"
        )
    
    def _extract_pk_from_row(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract primary key values from row data"""
        try:
            schema = self.get_table_schema(table)
            return {pk: data.get(pk) for pk in schema.primary_keys if pk in data}
        except:
            # Fallback: assume 'id' is primary key
            return {"id": data.get("id")} if "id" in data else {}
    
    def get_table_schema(self, table_name: str) -> TableSchema:
        """Get table schema from information_schema"""
        # Get columns
        self.cursor.execute(f"""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                IS_NULLABLE, 
                COLUMN_DEFAULT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = '{self.config["name"]}'
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = [
            {
                "name": row["COLUMN_NAME"],
                "type": row["DATA_TYPE"],
                "nullable": row["IS_NULLABLE"] == "YES",
                "default": row["COLUMN_DEFAULT"]
            }
            for row in self.cursor.fetchall()
        ]
        
        # Get primary keys
        self.cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '{self.config["name"]}'
            AND TABLE_NAME = '{table_name}'
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """)
        
        primary_keys = [row["COLUMN_NAME"] for row in self.cursor.fetchall()]
        
        return TableSchema(
            table_name=table_name,
            schema_name=self.config["name"],
            columns=columns,
            primary_keys=primary_keys,
            indexes=[]
        )
    
    def apply_change(self, event: ChangeEvent) -> bool:
        """Apply change event to MySQL"""
        try:
            if event.operation == OperationType.INSERT:
                # Build INSERT query
                columns = list(event.after.keys())
                values = list(event.after.values())
                placeholders = ", ".join(["%s"] * len(values))
                
                query = f"INSERT INTO {event.table} ({', '.join(columns)}) VALUES ({placeholders})"
                self.cursor.execute(query, values)
                
            elif event.operation == OperationType.UPDATE:
                # Build UPDATE query
                set_clause = ", ".join([f"{k} = %s" for k in event.after.keys()])
                where_clause = " AND ".join([f"{k} = %s" for k in event.primary_key.keys()])
                
                query = f"UPDATE {event.table} SET {set_clause} WHERE {where_clause}"
                params = list(event.after.values()) + list(event.primary_key.values())
                
                self.cursor.execute(query, params)
                
            elif event.operation == OperationType.DELETE:
                # Build DELETE query
                where_clause = " AND ".join([f"{k} = %s" for k in event.primary_key.keys()])
                
                query = f"DELETE FROM {event.table} WHERE {where_clause}"
                self.cursor.execute(query, list(event.primary_key.values()))
            
            return True
            
        except Exception as e:
            print(f"✗ Failed to apply change: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute SQL query"""
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def get_current_position(self) -> str:
        """Get current binlog position"""
        self.cursor.execute("SHOW MASTER STATUS")
        result = self.cursor.fetchone()
        
        if result:
            return f"{result['File']}:{result['Position']}"
        return "unknown:0"
    
    def get_binlog_info(self) -> Dict[str, Any]:
        """Get current binlog file and position"""
        self.cursor.execute("SHOW MASTER STATUS")
        result = self.cursor.fetchone()
        
        return {
            "file": result.get("File"),
            "position": result.get("Position"),
            "binlog_do_db": result.get("Binlog_Do_DB"),
            "binlog_ignore_db": result.get("Binlog_Ignore_DB")
        } if result else {}


# Register connector
ConnectorFactory.register("mysql", MySQLConnector)