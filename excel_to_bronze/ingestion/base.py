"""Base classes for data ingestion."""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict, Any

from excel_to_bronze.utils.logging import setup_logging


logger = setup_logging()


class DataIngestionError(Exception):
    """Base exception for data ingestion errors."""
    pass


class BaseIngestion(ABC):
    """Abstract base class for all ingestion processors."""
    
    @abstractmethod
    def process(self, data_source: Any, **kwargs) -> bool:
        """Process the data source and ingest data.
        
        Args:
            data_source: Source data to process
            **kwargs: Additional arguments for processing
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            DataIngestionError: If processing fails
        """
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate data before ingestion.
        
        Args:
            data: Data to validate
            
        Returns:
            True if validation passes, False otherwise
            
        Raises:
            DataIngestionError: If validation fails
        """
        pass


class FileIngestion(BaseIngestion):
    """Base class for file-based ingestion processors."""
    
    def __init__(self):
        """Initialize the file ingestion processor."""
        self.supported_extensions = []
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate DataFrame before ingestion.
        
        Args:
            data: DataFrame to validate
            
        Returns:
            True if validation passes
            
        Raises:
            DataIngestionError: If validation fails
        """
        # Basic validation to ensure DataFrame is not empty
        if data is None or data.empty:
            raise DataIngestionError("DataFrame is empty or None")
        return True
    
    def validate_file(self, file_path: str) -> bool:
        """Validate file before processing.
        
        Args:
            file_path: Path to file
            
        Returns:
            True if validation passes
            
        Raises:
            DataIngestionError: If validation fails
        """
        import os
        
        # Check file exists
        if not os.path.isfile(file_path):
            raise DataIngestionError(f"File does not exist: {file_path}")
        
        # Check file extension
        _, ext = os.path.splitext(file_path)
        if ext.lower() not in self.supported_extensions:
            raise DataIngestionError(
                f"Unsupported file extension: {ext}. "
                f"Supported: {', '.join(self.supported_extensions)}"
            )
        
        return True
    
    @abstractmethod
    def read_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Read data from file into DataFrame.
        
        Args:
            file_path: Path to file
            **kwargs: Additional arguments for file reading
            
        Returns:
            DataFrame with file data
        """
        pass
    
    @abstractmethod
    def write_data(self, data: pd.DataFrame, **kwargs) -> bool:
        """Write data to destination.
        
        Args:
            data: DataFrame to write
            **kwargs: Additional arguments for writing
            
        Returns:
            True if successful
        """
        pass
    
    def process(self, file_path: str, **kwargs) -> bool:
        """Process the file and ingest data.
        
        Args:
            file_path: Path to file
            **kwargs: Additional processing arguments
            
        Returns:
            True if successful
            
        Raises:
            DataIngestionError: If processing fails
        """
        try:
            # Validate file
            self.validate_file(file_path)
            
            # Read file
            data = self.read_file(file_path, **kwargs)
            
            # Validate data
            self.validate(data)
            
            # Write data
            return self.write_data(data, **kwargs)
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise DataIngestionError(f"Failed to process file: {str(e)}")