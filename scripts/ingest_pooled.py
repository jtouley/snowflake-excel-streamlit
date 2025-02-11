import json
import pandas as pd
import snowflake.connector
import yaml
import os
from datetime import datetime, timedelta  # Import timedelta from datetime
from typing import Dict, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeIngestor:
    """Handles ingestion of Excel files into Snowflake Iceberg bronze table."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.snowflake_config = self._load_config(config_path)
        self.connection = None
        
    def _load_config(self, config_path: str) -> Dict[str, str]:
        """Load Snowflake configuration from environment or YAML."""
        config = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }
        
        if not config["account"] and os.path.exists(config_path):
            with open(config_path, "r") as f:
                yaml_config = yaml.safe_load(f)["snowflake"]
                config.update(yaml_config)
        
        return config

    def _connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            self.connection = snowflake.connector.connect(
                user=self.snowflake_config["user"],
                password=self.snowflake_config["password"],
                account=self.snowflake_config["account"],
                warehouse=self.snowflake_config["warehouse"],
                database=self.snowflake_config["database"],
                schema=self.snowflake_config["schema"]
            )
            logger.info("Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def _serialize_data(self, df: pd.DataFrame) -> pd.Series:
        """Serialize DataFrame rows to JSON strings, handling all data types."""
        def convert_value(value: Any) -> Any:
            if pd.isna(value):
                return None
            elif isinstance(value, (pd.Timestamp, datetime)):
                return value.isoformat()
            # Handle both pandas Timedelta and Python's built-in timedelta
            elif isinstance(value, (pd.Timedelta, timedelta)):
                return str(value)
            elif hasattr(value, 'item'):  # Handle numpy types
                return value.item()
            return value

        def row_to_json(row: pd.Series) -> str:
            row_dict = {
                'metadata': {
                    'column_names': row.index.tolist(),
                    'dtypes': {col: str(row[col].__class__.__name__) for col in row.index}
                },
                'data': {str(k): convert_value(v) for k, v in row.items()}
            }
            return json.dumps(row_dict)

        return df.apply(row_to_json, axis=1)

    def ingest_excel(self, file_path: str) -> None:
        """Process Excel file and upload to Snowflake bronze table in batches."""
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            filename = os.path.basename(file_path)
            
            # Add unique ID for each row
            df['id'] = df.index.astype(str)
            
            # Connect to Snowflake if not already connected
            if not self.connection:
                self._connect()
            
            cursor = self.connection.cursor()
            
            # Convert DataFrame rows to JSON strings
            raw_data_series = self._serialize_data(df)
            
            # Prepare data for insertion as a list of tuples (id, filename, raw_data)
            rows_to_insert = [(str(idx), filename, raw_data) 
                              for idx, raw_data in enumerate(raw_data_series)]
            
            # Batch size: up to 1000 rows at a time
            batch_size = 10000
            insert_sql = """
                INSERT INTO bronze_table (id, filename, uploaded_at, raw_data)
                VALUES (%s, %s, CURRENT_TIMESTAMP(), %s)
            """
            
            # Process the rows in batches
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                self.connection.commit()
                logger.info(f"Inserted batch {i // batch_size + 1} with {len(batch)} rows")
            
            logger.info(f"Successfully ingested {filename} to bronze layer")
            
        except Exception as e:
            logger.error(f"Error ingesting file {file_path}: {e}")
            raise
        finally:
            if self.connection:
                self.connection.close()
                self.connection = None

def main():
    """Main execution function."""
    import sys
    if len(sys.argv) < 2:
        logger.error("Usage: python scripts/ingest.py <file.xlsx>")
        sys.exit(1)
    
    ingestor = BronzeIngestor()
    ingestor.ingest_excel(sys.argv[1])

if __name__ == "__main__":
    main()