"""
Event handler for processing CDC events
Includes conflict resolution for bidirectional sync
"""
import time
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import json

from src.connectors.base import ChangeEvent, OperationType, ConnectorFactory
from config.settings import get_settings


class ConflictResolver:
    """
    Resolves conflicts in bidirectional synchronization
    """
    
    def __init__(self, strategy: str = "last_write_wins"):
        self.strategy = strategy
    
    def resolve(
        self, 
        local_event: ChangeEvent, 
        remote_event: ChangeEvent
    ) -> Optional[ChangeEvent]:
        """
        Resolve conflict between two events
        
        Args:
            local_event: Event from local database
            remote_event: Event from remote database
            
        Returns:
            The winning event or None if conflict cannot be resolved
        """
        if self.strategy == "last_write_wins":
            # Use timestamp to determine winner
            if local_event.timestamp > remote_event.timestamp:
                return local_event
            else:
                return remote_event
        
        elif self.strategy == "source_priority":
            # Always prioritize local/source
            return local_event
        
        elif self.strategy == "target_priority":
            # Always prioritize remote/target
            return remote_event
        
        return None


class EventHandler:
    """
    Handles CDC events and applies them to target database
    """
    
    def __init__(self, source_connector, target_connector, conflict_resolver):
        self.source = source_connector
        self.target = target_connector
        self.resolver = conflict_resolver
        self.processed_count = 0
        self.error_count = 0
    
    def process_event(self, event: ChangeEvent) -> bool:
        """
        Process a single change event
        
        Args:
            event: Change event to process
            
        Returns:
            bool: True if successful
        """
        try:
            # Check for potential conflicts
            if event.operation == OperationType.UPDATE:
                # Could check if target has newer version
                pass
            
            # Apply change to target
            success = self.target.apply_change(event)
            
            if success:
                self.processed_count += 1
                print(f"✓ Processed {event.operation.value} on {event.table}")
            else:
                self.error_count += 1
                print(f"✗ Failed to process {event.operation.value} on {event.table}")
            
            return success
            
        except Exception as e:
            self.error_count += 1
            print(f"✗ Error processing event: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics"""
        return {
            "processed": self.processed_count,
            "errors": self.error_count
        }


class OffsetManager:
    """
    Manages replication offsets for crash recovery
    """
    
    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.offset_file = self.storage_path / "offsets.json"
    
    def save_offset(self, connector_name: str, position: str) -> None:
        """Save current offset"""
        offsets = self.load_offsets()
        offsets[connector_name] = {
            "position": position,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(self.offset_file, "w") as f:
            json.dump(offsets, f, indent=2)
    
    def load_offsets(self) -> Dict[str, Any]:
        """Load saved offsets"""
        if self.offset_file.exists():
            with open(self.offset_file, "r") as f:
                return json.load(f)
        return {}
    
    def get_offset(self, connector_name: str) -> Optional[str]:
        """Get last saved offset for a connector"""
        offsets = self.load_offsets()
        if connector_name in offsets:
            return offsets[connector_name]["position"]
        return None