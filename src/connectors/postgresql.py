"""
PostgreSQL CDC Connector using logical replication
Reads from WAL using pgoutput plugin
"""
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import LogicalReplicationConnection, ReplicationMessage
from typing import Dict, Any, List, Optional, Iterator
from datetime import datetime

from .base import (
    BaseConnector, ChangeEvent, TableSchema, 
    OperationType, ConnectorFactory
)


class PostgreSQLConnector(BaseConnector):
    """
    PostgreSQL CDC connector using logical replication
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.conn = None
        self.repl_conn = None
        self.cursor = None
        self.repl_cursor = None
        
        self.slot_name = config.get("slot_name", "cdc_slot")
        self.publication = config.get("publication", "cdc_publication")
    
    def connect(self) -> None:
        """Establish connection to PostgreSQL"""
        try:
            # Regular connection for queries
            self.conn = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["name"],
                user=self.config["user"],
                password=self.config["password"]
            )
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            
            # Replication connection for CDC
            self.repl_conn = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["name"],
                user=self.config["user"],
                password=self.config["password"],
                connection_factory=LogicalReplicationConnection
            )
            self.repl_cursor = self.repl_conn.cursor()
            
            self.connected = True
            print(f"✓ Connected to PostgreSQL: {self.config['host']}:{self.config['port']}")
            
        except Exception as e:
            print(f"✗ Failed to connect to PostgreSQL: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close PostgreSQL connections"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        if self.repl_cursor:
            self.repl_cursor.close()
        if self.repl_conn:
            self.repl_conn.close()
        
        self.connected = False
        print("Disconnected from PostgreSQL")
    
    def setup_cdc(self, tables: List[str]) -> None:
        """
        Set up CDC infrastructure
        Creates replication slot and publication
        """
        try:
            # Check if replication slot exists
            self.cursor.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (self.slot_name,)
            )
            
            if not self.cursor.fetchone():
                # Create replication slot
                self.cursor.execute(
                    sql.SQL("SELECT pg_create_logical_replication_slot(%s, %s)"),
                    (self.slot_name, "pgoutput")
                )
                print(f"✓ Created replication slot: {self.slot_name}")
            else:
                print(f"✓ Replication slot already exists: {self.slot_name}")
            
            # Check if publication exists
            self.cursor.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s",
                (self.publication,)
            )
            
            if not self.cursor.fetchone():
                # Create publication for specific tables
                tables_list = ", ".join(tables)
                self.cursor.execute(
                    sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                        sql.Identifier(self.publication),
                        sql.SQL(tables_list)
                    )
                )
                print(f"✓ Created publication: {self.publication} for tables: {tables_list}")
            else:
                print(f"✓ Publication already exists: {self.publication}")
                
        except Exception as e:
            print(f"✗ Failed to setup CDC: {e}")
            raise
    
    def start_streaming(self, start_lsn: Optional[str] = None) -> Iterator[ChangeEvent]:
        """
        Start streaming change events from WAL
        
        Args:
            start_lsn: Starting LSN position (format: '0/ABC123')
            
        Yields:
            ChangeEvent: Change events from WAL
        """
        try:
            # If no start_lsn, fetch current WAL position
            if not start_lsn:
                self.cursor.execute("SELECT COALESCE(pg_current_wal_lsn(), '0/0')")
                start_lsn = self.cursor.fetchone()[0]

            self.repl_cursor.start_replication(
                slot_name=self.slot_name,
                decode=True,
                start_lsn=start_lsn or 0/0,
                options={
                    "proto_version": "1",
                    "publication_names": self.publication
                }
            )
            
            print(f"✓ Started streaming from LSN: {start_lsn}")
            self.repl_cursor.consume_stream(self._process_message)
            
        except Exception as e:
            print(f"✗ Streaming error: {e}")
            raise
    
    def _process_message(self, msg: ReplicationMessage):
        """
        Process individual replication message
        """
        lsn = msg.data_start or "0/0"

        if msg.payload:
            try:
                data = json.loads(msg.payload)
                event = self._parse_wal_data(data, lsn)
                if event:
                    print(f"Event: {event}")
            except json.JSONDecodeError:
                print(f"Failed to parse message: {msg.payload}")

        # Send feedback to server
        if msg.data_start is not None:
            msg.cursor.send_feedback(flush_lsn=msg.daata_start)
        else:
            print("Empty message recieved, skipping..")
    
    def _parse_wal_data(self, data: Dict[str, Any], lsn: str) -> Optional[ChangeEvent]:
        action = data.get("action")
        
        if action == "I":  # INSERT
            return ChangeEvent(
                operation=OperationType.INSERT,
                table=data.get("table"),
                schema=data.get("schema"),
                timestamp=datetime.now(),
                before=None,
                after=data.get("columns"),
                primary_key=self._extract_pk(data.get("columns")),
                lsn=lsn,
                source_db="postgresql"
            )
        
        elif action == "U":  # UPDATE
            return ChangeEvent(
                operation=OperationType.UPDATE,
                table=data.get("table"),
                schema=data.get("schema"),
                timestamp=datetime.now(),
                before=data.get("identity"),
                after=data.get("columns"),
                primary_key=self._extract_pk(data.get("identity")),
                lsn=lsn,
                source_db="postgresql"
            )
        
        elif action == "D":  # DELETE
            return ChangeEvent(
                operation=OperationType.DELETE,
                table=data.get("table"),
                schema=data.get("schema"),
                timestamp=datetime.now(),
                before=data.get("identity"),
                after=None,
                primary_key=self._extract_pk(data.get("identity")),
                lsn=lsn,
                source_db="postgresql"
            )
        
        return None
    
    def _extract_pk(self, data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not data:
            return {}
        return {k: v for k, v in data.items() if k.endswith("_id") or k == "id"}
    
    def get_table_schema(self, table_name: str) -> TableSchema:
        self.cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = [
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES",
                "default": row[3]
            }
            for row in self.cursor.fetchall()
        ]
        
        self.cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """, (table_name,))
        
        primary_keys = [row[0] for row in self.cursor.fetchall()]
        
        return TableSchema(
            table_name=table_name,
            schema_name="public",
            columns=columns,
            primary_keys=primary_keys,
            indexes=[]
        )
    
    def apply_change(self, event: ChangeEvent) -> bool:
        try:
            if event.operation == OperationType.INSERT:
                columns = list(event.after.keys())
                values = list(event.after.values())
                query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(event.table),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() * len(values))
                )
                self.cursor.execute(query, values)
                
            elif event.operation == OperationType.UPDATE:
                set_clause = sql.SQL(", ").join(
                    sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
                    for k in event.after.keys()
                )
                where_clause = sql.SQL(" AND ").join(
                    sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
                    for k in event.primary_key.keys()
                )
                query = sql.SQL("UPDATE {} SET {} WHERE {}").format(
                    sql.Identifier(event.table),
                    set_clause,
                    where_clause
                )
                params = list(event.after.values()) + list(event.primary_key.values())
                self.cursor.execute(query, params)
                
            elif event.operation == OperationType.DELETE:
                where_clause = sql.SQL(" AND ").join(
                    sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
                    for k in event.primary_key.keys()
                )
                query = sql.SQL("DELETE FROM {} WHERE {}").format(
                    sql.Identifier(event.table),
                    where_clause
                )
                self.cursor.execute(query, list(event.primary_key.values()))
            
            return True
            
        except Exception as e:
            print(f"✗ Failed to apply change: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Any:
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def get_current_position(self) -> str:
        self.cursor.execute("SELECT pg_current_wal_lsn()")
        return self.cursor.fetchone()[0]


# Register connector
ConnectorFactory.register("postgresql", PostgreSQLConnector)
