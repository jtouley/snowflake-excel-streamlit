"""Snowflake connection management."""
from contextlib import contextmanager
from typing import Any, Dict, Optional

import snowflake.connector

from excel_to_bronze.config import config
from excel_to_bronze.utils.logging import setup_logging

logger = setup_logging()


class SnowflakeConnector:
    """Manages connections to Snowflake with connection pooling support."""

    _instance = None

    def __new__(cls):
        """Singleton pattern to ensure only one connector instance exists."""
        if cls._instance is None:
            cls._instance = super(SnowflakeConnector, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize Snowflake connector."""
        # Skip initialization if already done (singleton pattern)
        if getattr(self, "_initialized", False):
            return

        self.snowflake_config = config.get_snowflake_config()
        self.connection_pool = {}
        self._initialized = True

    @contextmanager
    def get_connection(self):
        """Get a Snowflake connection using context manager pattern.

        Usage:
            with snowflake_connector.get_connection() as conn:
                # Use connection
        """
        connection = None
        try:
            # Establish connection
            connection = snowflake.connector.connect(
                user=self.snowflake_config["user"],
                password=self.snowflake_config["password"],
                account=self.snowflake_config["account"],
                warehouse=self.snowflake_config["warehouse"],
                database=self.snowflake_config["database"],
                schema=self.snowflake_config["schema"],
            )
            logger.debug("Connected to Snowflake successfully")
            yield connection
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
        finally:
            # Close connection when done
            if connection:
                connection.close()
                logger.debug("Closed Snowflake connection")

    def execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> list:
        """Execute a SQL query on Snowflake.

        Args:
            sql: SQL query to execute
            params: Parameters for the query

        Returns:
            Query results as a list of records
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or {})
                return cursor.fetchall()
            finally:
                cursor.close()

    def execute_batch(self, sql: str, params_list: list) -> None:
        """Execute a batch of SQL statements on Snowflake.

        Args:
            sql: SQL query to execute
            params_list: List of parameter sets for the query
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, params_list)
                conn.commit()
            finally:
                cursor.close()


# Default instance
snowflake_connector = SnowflakeConnector()
