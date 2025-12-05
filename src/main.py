"""
Main application entry point
Orchestrates bidirectional database synchronization
"""
import sys
import signal
from time import sleep
from config.settings import get_settings
from src.connectors.base import ConnectorFactory
from src.handlers.event_handler import ConflictResolver, EventHandler, OffsetManager
from src.connectors import mysql
from src.connectors import postgresql
from dotenv import load_dotenv
load_dotenv()

def main():
    """Main application loop"""
    print("=" * 70)
    print("DATABASE BIDIRECTIONAL SYNC SYSTEM")
    print("=" * 70)
    
    # Load settings
    settings = get_settings()
    
    if not settings.validate():
        print("\n✗ Configuration validation failed")
        sys.exit(1)
    
    print(f"\n{settings}\n")
    
    # Initialize offset manager
    offset_mgr = OffsetManager(settings.storage.offset_storage_path)
    
    # Create database connectors
    try:
        print("\n--- Initializing Connectors ---")
        
        source_config = {
            "host": settings.source_db.host,
            "port": settings.source_db.port,
            "name": settings.source_db.name,
            "user": settings.source_db.user,
            "password": settings.source_db.password,
            "slot_name": settings.source_db.slot_name,
            "publication": settings.source_db.publication,
        }
        
        target_config = {
            "host": settings.target_db.host,
            "port": settings.target_db.port,
            "name": settings.target_db.name,
            "user": settings.target_db.user,
            "password": settings.target_db.password,
        }
        
        source = ConnectorFactory.create("postgresql", source_config)
        target = ConnectorFactory.create("mysql", target_config)
        
    except Exception as e:
        print(f"\n✗ Failed to create connectors: {e}")
        sys.exit(1)
    
    # Connect to databases
    try:
        print("\n--- Connecting to Databases ---")
        source.connect()
        target.connect()
        
    except Exception as e:
        print(f"\n✗ Connection failed: {e}")
        sys.exit(1)
    
    # Setup CDC
    try:
        print("\n--- Setting Up CDC Infrastructure ---")
        tables = settings.sync.tables_list
        source.setup_cdc(tables)
        
        if settings.sync.enable_bidirectional:
            print("\n✓ Bidirectional sync enabled")
            if settings.target_db.type == "postgresql":
                target.setup_cdc(tables)
        
    except Exception as e:
        print(f"\n✗ CDC setup failed: {e}")
        source.disconnect()
        target.disconnect()
        sys.exit(1)
    
    # Initialize event handler
    resolver = ConflictResolver(settings.sync.conflict_resolution)
    handler = EventHandler(source, target, resolver)
    
    # Graceful shutdown handler
    def signal_handler(sig, frame):
        print("\n\n--- Shutting Down ---")
        print(f"Statistics: {handler.get_stats()}")
        source.disconnect()
        target.disconnect()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start synchronization
    print("\n--- Starting Synchronization ---")
    print("Press Ctrl+C to stop\n")
    
    try:
        # Get last offset
        last_offset = offset_mgr.get_offset(f"source_{settings.source_db.type}")
        
        # Start streaming from source
        print(f"Streaming from: {last_offset or 'beginning'}")
        
        event_stream = source.start_streaming(last_offset)
        
        for event in event_stream:
            # Process event
            handler.process_event(event)
            
            # Save offset periodically
            if handler.processed_count % 100 == 0:
                current_pos = source.get_current_position()
                offset_mgr.save_offset(
                    f"source_{settings.source_db.type}", 
                    current_pos
                )
            
            # Rate limiting
            sleep(settings.sync.sync_interval_seconds)
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n✗ Sync error: {e}")
    finally:
        print("\n--- Final Statistics ---")
        print(f"  Processed: {handler.processed_count}")
        print(f"  Errors: {handler.error_count}")
        
        source.disconnect()
        target.disconnect()


if __name__ == "__main__":
    main()