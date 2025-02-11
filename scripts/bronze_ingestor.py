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
    
    def ingest_excel(self, file_path: str) -> None:
        """Ingests an Excel file into the Snowflake Bronze layer."""
        try:
            self.ingestor.ingest_excel(file_path)
            logger.info(f"✅ Successfully processed {file_path}")
        except Exception as e:
            logger.error(f"❌ Error processing {file_path}: {str(e)}")
            raise