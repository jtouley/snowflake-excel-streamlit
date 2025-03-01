"""Bronze layer ingestion implementation."""
import os
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple

from excel_to_bronze.config import config
from excel_to_bronze.connectors.snowflake import snowflake_connector
from excel_to_bronze.ingestion.base import FileIngestion, DataIngestionError
from excel_to_bronze.ingestion.serializers import DataSerializer
from excel_to_bronze.utils.logging import setup_logging


logger = setup_logging()


class ExcelIngestor(FileIngestion):
    """Excel file ingestion processor for the Bronze layer."""
    
    def __init__(self):
        """Initialize the Excel ingestion processor."""
        super().__init__()
        self.supported_extensions = ['.xlsx', '.xls']
        self.batch_size = config.get_batch_size()
        self.bronze_table = config.get_bronze_table()
    
    def read_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Read Excel file into DataFrame.
        
        Args:
            file_path: Path to Excel file
            **kwargs: Additional arguments for pd.read_excel
            
        Returns:
            DataFrame with Excel data
        """
        try:
            df = pd.read_excel(file_path, **kwargs)
            logger.info(f"Read {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            raise DataIngestionError(f"Excel file read error: {str(e)}")
    
    def prepare_data(self, df: pd.DataFrame, filename: str) -> List[Tuple]:
        """Prepare DataFrame for insertion into bronze table.
        
        Args:
            df: DataFrame to prepare
            filename: Original filename to store
            
        Returns:
            List of tuples ready for insertion
        """
        # Add unique ID for each row
        df = DataSerializer.add_metadata(
            df, {'id': lambda df: df.index.astype(str)}
        )
        
        # Convert DataFrame rows to JSON strings
        raw_data_series = DataSerializer.serialize_dataframe(df)
        
        # Create list of tuples for batch insertion
        return [(str(idx), filename, raw_data) 
                for idx, raw_data in enumerate(raw_data_series)]
    
    def write_data(self, df: pd.DataFrame, **kwargs) -> bool:
        """Write DataFrame to Snowflake bronze table.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional arguments including filename
            
        Returns:
            True if successful
            
        Raises:
            DataIngestionError: If write fails
        """
        try:
            # Get original filename
            filename = kwargs.get('original_filename', 
                       kwargs.get('filename', 'unknown.xlsx'))
            
            # Prepare data for insertion
            rows_to_insert = self.prepare_data(df, filename)
            
            # Insert SQL statement
            insert_sql = f"""
                INSERT INTO {self.bronze_table} (id, filename, uploaded_at, raw_data)
                VALUES (%s, %s, CURRENT_TIMESTAMP(), %s)
            """
            
            # Process in batches
            total_batches = (len(rows_to_insert) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(rows_to_insert), self.batch_size):
                batch = rows_to_insert[i:i + self.batch_size]
                snowflake_connector.execute_batch(insert_sql, batch)
                logger.info(f"Inserted batch {i // self.batch_size + 1}/{total_batches} "
                            f"with {len(batch)} rows")
            
            logger.info(f"Successfully ingested {filename} to bronze layer")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write data to bronze layer: {e}")
            raise DataIngestionError(f"Bronze layer write error: {str(e)}")
    
    def ingest_excel(self, file_path: str, original_filename: Optional[str] = None) -> bool:
        """Ingest Excel file into bronze layer.
        
        Args:
            file_path: Path to Excel file
            original_filename: Original filename to preserve
            
        Returns:
            True if successful
            
        Raises:
            DataIngestionError: If ingestion fails
        """
        # Use provided original filename or extract from path
        filename = original_filename or os.path.basename(file_path)
        
        return self.process(file_path, original_filename=filename)

    @staticmethod
    def ingest_excel_file(file_path: str, original_filename: Optional[str] = None) -> bool:
        """Static helper method to ingest Excel file.
        
        Args:
            file_path: Path to Excel file
            original_filename: Original filename to preserve
            
        Returns:
            True if successful
        """
        ingestor = ExcelIngestor()
        return ingestor.ingest_excel(file_path, original_filename)