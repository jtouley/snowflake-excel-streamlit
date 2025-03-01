"""Configuration management for Excel to Bronze application."""
import os
import yaml
from typing import Dict, Any, Optional


class ConfigManager:
    """Manages application configuration from environment variables and config files."""
    
    _instance = None
    
    def __new__(cls, config_path: Optional[str] = None):
        """Singleton pattern to ensure only one config instance exists."""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration from environment variables or config file."""
        # Skip initialization if already done (singleton pattern)
        if self._initialized:
            return
            
        self.config_path = config_path or os.getenv("CONFIG_PATH", "config/config.yaml")
        self.config = self._load_config()
        self._initialized = True
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables or YAML file."""
        # Default configuration structure
        config = {
            "snowflake": {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            },
            "application": {
                "log_level": os.getenv("LOG_LEVEL", "INFO"),
                "batch_size": int(os.getenv("BATCH_SIZE", "10000")),
                "bronze_table": os.getenv("BRONZE_TABLE", "bronze_table")
            }
        }
        
        # Override with config file if it exists
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r") as f:
                    yaml_config = yaml.safe_load(f)
                    
                # Merge configuration
                if yaml_config:
                    # Update snowflake config if present
                    if "snowflake" in yaml_config:
                        config["snowflake"].update(yaml_config["snowflake"])
                    # Update application config if present
                    if "application" in yaml_config:
                        config["application"].update(yaml_config["application"])
            except Exception as e:
                print(f"Error loading configuration file: {e}")
        
        return config
    
    def get_snowflake_config(self) -> Dict[str, str]:
        """Get Snowflake connection configuration."""
        return self.config["snowflake"]
    
    def get_application_config(self) -> Dict[str, Any]:
        """Get application configuration."""
        return self.config["application"]
    
    def get_batch_size(self) -> int:
        """Get batch size for data processing."""
        return self.config["application"]["batch_size"]
    
    def get_bronze_table(self) -> str:
        """Get bronze table name."""
        return self.config["application"]["bronze_table"]


# Default instance
config = ConfigManager()