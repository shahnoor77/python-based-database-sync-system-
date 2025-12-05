"""
Configuration management using Pydantic Settings
Loads configuration from environment variables
"""
from typing import List, Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseSettings):
    """Database connection configuration"""
    
    type: Literal["postgresql", "mysql"]
    host: str
    port: int
    name: str 
    user: str
    password: str
    
    # PostgreSQL specific
    slot_name: str = "cdc_slot_source"
    publication: str = "cdc_publication_source"
    
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")


class SourceDatabaseConfig(DatabaseConfig):
    """Source database configuration"""
    
    model_config = SettingsConfigDict(env_prefix="SOURCE_DB_", extra="ignore")


class TargetDatabaseConfig(DatabaseConfig):
    """Target database configuration"""
    
    model_config = SettingsConfigDict(env_prefix="TARGET_DB_", extra="ignore")


class SyncConfig(BaseSettings):
    """Synchronization configuration"""
    
    enable_bidirectional: bool = Field(default=True)
    conflict_resolution: Literal[
        "last_write_wins", 
        "source_priority", 
        "target_priority"
    ] = "last_write_wins"
    
    tables_to_sync: str = ""  # Comma-separated list
    batch_size: int = 1000
    max_retries: int = 3
    sync_interval_seconds: int = 1
    
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")
    
    @property
    def tables_list(self) -> List[str]:
        """Parse comma-separated tables into list"""
        if not self.tables_to_sync:
            return []
        return [t.strip() for t in self.tables_to_sync.split(",") if t.strip()]


class StorageConfig(BaseSettings):
    """Storage configuration for offsets and schemas"""
    
    offset_storage_path: str = "./data/offsets"
    schema_storage_path: str = "./data/schemas"
    
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")


class MonitoringConfig(BaseSettings):
    """Monitoring and metrics configuration"""
    
    enable_metrics: bool = True
    metrics_port: int = 9090
    
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")


class AppConfig(BaseSettings):
    """Main application configuration"""
    
    app_name: str = "db-sync-system"
    log_level: str = "INFO"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


class Settings:
    """
    Central settings manager that loads all configurations
    """
    
    def __init__(self):
        self.app = AppConfig()
        self.source_db = SourceDatabaseConfig()
        self.target_db = TargetDatabaseConfig()
        self.sync = SyncConfig()
        self.storage = StorageConfig()
        self.monitoring = MonitoringConfig()
    
    def validate(self) -> bool:
        """
        Validate configuration
        
        Returns:
            bool: True if configuration is valid
        """
        errors = []
        
        # Validate database types
        valid_types = {"postgresql", "mysql"}
        if self.source_db.type not in valid_types:
            errors.append(f"Invalid source DB type: {self.source_db.type}")
        if self.target_db.type not in valid_types:
            errors.append(f"Invalid target DB type: {self.target_db.type}")
        
        # Validate tables to sync
        if not self.sync.tables_list:
            errors.append("No tables specified for synchronization")
        
        # Validate batch size
        if self.sync.batch_size < 1:
            errors.append("Batch size must be at least 1")
        
        # Validate sync interval
        if self.sync.sync_interval_seconds < 0:
            errors.append("Sync interval must be non-negative")
        
        if errors:
            for error in errors:
                print(f"Configuration Error: {error}")
            return False
        
        return True
    
    def get_database_config(self, db_type: Literal["source", "target"]) -> DatabaseConfig:
        """
        Get database configuration by type
        
        Args:
            db_type: Either 'source' or 'target'
            
        Returns:
            DatabaseConfig: The requested database configuration
        """
        if db_type == "source":
            return self.source_db
        elif db_type == "target":
            return self.target_db
        else:
            raise ValueError(f"Invalid database type: {db_type}")
    
    def __repr__(self) -> str:
        return (
            f"Settings(\n"
            f"  app={self.app.app_name},\n"
            f"  source_db={self.source_db.type}://{self.source_db.host}:{self.source_db.port}/{self.source_db.name},\n"
            f"  target_db={self.target_db.type}://{self.target_db.host}:{self.target_db.port}/{self.target_db.name},\n"
            f"  tables={self.sync.tables_list},\n"
            f"  bidirectional={self.sync.enable_bidirectional}\n"
            f")"
        )


# Singleton instance
_settings_instance = None


def get_settings() -> Settings:
    """
    Get or create settings singleton
    
    Returns:
        Settings: The global settings instance
    """
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance


# Example usage
if __name__ == "__main__":
    settings = get_settings()
    
    print("=" * 60)
    print("DATABASE SYNC SYSTEM CONFIGURATION")
    print("=" * 60)
    print(settings)
    print("=" * 60)
    
    if settings.validate():
        print("✓ Configuration is valid")
    else:
        print("✗ Configuration has errors")