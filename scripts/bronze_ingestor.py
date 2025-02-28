"""Bronze layer ingestion handler for Excel files."""
import logging
from scripts.ingest_pooled import BronzeIngestor as PooledIngestor

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeIngestor:
    """Handles Excel ingestion into Snowflake Bronze Layer."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.ingestor = PooledIngestor(config_path)
    
    def ingest_excel(self, file_path: str, original_filename: str = None) -> None:
        """Ingests an Excel file into the Snowflake Bronze layer.
        
        Args:
            file_path: Path to the Excel file to process
            original_filename: Original filename to preserve in the database
        """
        try:
            self.ingestor.ingest_excel(file_path, original_filename)
            logger.info(f"✅ Successfully processed {original_filename or file_path}")
        except Exception as e:
            logger.error(f"❌ Error processing {original_filename or file_path}: {str(e)}")
            raise