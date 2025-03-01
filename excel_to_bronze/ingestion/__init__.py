"""Data ingestion module."""
from excel_to_bronze.ingestion.base import BaseIngestion, FileIngestion, DataIngestionError
from excel_to_bronze.ingestion.bronze import ExcelIngestor
