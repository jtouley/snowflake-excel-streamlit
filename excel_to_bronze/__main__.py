"""Command-line interface for Excel to Bronze ingestion."""
import argparse
import sys

from excel_to_bronze.ingestion.base import DataIngestionError
from excel_to_bronze.ingestion.bronze import ExcelIngestor
from excel_to_bronze.utils.logging import setup_logging

# Set up logging
logger = setup_logging()


def main():
    """Main command-line interface."""
    parser = argparse.ArgumentParser(
        description="Ingest Excel files into Snowflake Bronze layer."
    )
    parser.add_argument("file_path", type=str, help="Path to Excel file for ingestion")
    parser.add_argument(
        "--filename",
        type=str,
        help="Original filename to store (defaults to basename of file_path)",
    )
    parser.add_argument("--config", type=str, help="Path to configuration file")

    args = parser.parse_args()

    try:
        # Initialize ingestor
        ingestor = ExcelIngestor()

        # Process file
        logger.info(f"Processing file: {args.file_path}")
        result = ingestor.ingest_excel(args.file_path, args.filename)

        if result:
            logger.info(f"Successfully ingested {args.file_path}")
            return 0
        else:
            logger.error(f"Failed to ingest {args.file_path}")
            return 1

    except DataIngestionError as e:
        logger.error(f"Ingestion error: {str(e)}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
