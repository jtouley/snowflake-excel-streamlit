"""Data serialization utilities for ingestion pipeline."""
import json
from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd


class DataSerializer:
    """Handles serialization of DataFrame data to JSON format."""

    @staticmethod
    def convert_value(value: Any) -> Any:
        """Convert value to JSON-serializable format.

        Args:
            value: The value to convert

        Returns:
            JSON-serializable version of the value
        """
        if pd.isna(value):
            return None
        elif isinstance(value, (pd.Timestamp, datetime)):
            return value.isoformat()
        elif isinstance(value, (pd.Timedelta, timedelta)):
            return str(value)
        elif hasattr(value, "item"):  # Handle numpy types
            return value.item()
        return value

    @classmethod
    def row_to_json(cls, row: pd.Series) -> str:
        """Convert a DataFrame row to a JSON string.

        Args:
            row: DataFrame row as a Series

        Returns:
            JSON string representation with metadata
        """
        row_dict = {
            "metadata": {
                "column_names": row.index.tolist(),
                "dtypes": {col: str(row[col].__class__.__name__) for col in row.index},
            },
            "data": {str(k): cls.convert_value(v) for k, v in row.items()},
        }
        return json.dumps(row_dict)

    @classmethod
    def serialize_dataframe(cls, df: pd.DataFrame) -> pd.Series:
        """Serialize a DataFrame to a Series of JSON strings.

        Args:
            df: DataFrame to serialize

        Returns:
            Series of JSON strings
        """
        return df.apply(cls.row_to_json, axis=1)

    @staticmethod
    def add_metadata(df: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
        """Add metadata columns to DataFrame.

        Args:
            df: DataFrame to enhance
            metadata: Dictionary of metadata to add

        Returns:
            Enhanced DataFrame with metadata columns
        """
        df_copy = df.copy()
        for key, value in metadata.items():
            if callable(value):
                df_copy[key] = value(df_copy)
            else:
                df_copy[key] = value
        return df_copy
