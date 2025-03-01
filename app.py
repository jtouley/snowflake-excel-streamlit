"""Streamlit application for Excel to Snowflake Bronze ingestion."""
import os
import tempfile
import time

import pandas as pd
import streamlit as st

from excel_to_bronze.ingestion.base import DataIngestionError
from excel_to_bronze.ingestion.bronze import ExcelIngestor
from excel_to_bronze.utils.logging import setup_logging

# Set up logging
logger = setup_logging()

# 1. Track processed files in session state to avoid re-ingestion.
#    This prevents reloading both files when a new one is added.
if "processed_files" not in st.session_state:
    st.session_state.processed_files = set()


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Excel to Snowflake Bronze Ingestion", page_icon="📊", layout="wide"
    )

    st.title("Excel to Snowflake Bronze Layer Ingestion")
    st.markdown(
        """
    Upload Excel files to be ingested into the Snowflake bronze layer.
    Files will be processed and stored as JSON in an Iceberg table.
    """
    )

    # File uploader
    uploaded_files = st.file_uploader(
        "Choose Excel file(s)", type=["xlsx", "xls"], accept_multiple_files=True
    )

    if uploaded_files:
        ingestor = ExcelIngestor()

        # Progress tracking
        progress_container = st.empty()
        status_container = st.empty()
        preview_container = st.empty()

        for uploaded_file in uploaded_files:
            # Skip files that have already been processed
            if uploaded_file.name in st.session_state.processed_files:
                st.info(f"{uploaded_file.name} has already been processed. Skipping.")
                continue

            try:
                # Create progress bar
                progress_bar = progress_container.progress(0)
                status_container.info(f"Processing {uploaded_file.name}...")

                # Preview the data
                df = pd.read_excel(uploaded_file)
                with preview_container.expander("Preview Data"):
                    st.dataframe(df.head())
                    st.text(f"Total rows: {len(df)}")
                    st.text(f"Columns: {', '.join(df.columns)}")

                # Create a temporary file
                with tempfile.NamedTemporaryFile(
                    delete=False, suffix=".xlsx"
                ) as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_file_path = tmp_file.name

                # Process the file with original filename
                ingestor.ingest_excel(
                    tmp_file_path, original_filename=uploaded_file.name
                )

                # Update progress
                progress_bar.progress(100)
                status_container.success(f"Successfully processed {uploaded_file.name}")

                # Clean up
                os.unlink(tmp_file_path)
                time.sleep(1)  # Give users time to see the success message

                # Mark file as processed so it won't be re-ingested.
                st.session_state.processed_files.add(uploaded_file.name)

            except DataIngestionError as e:
                status_container.error(
                    f"Error processing {uploaded_file.name}: {str(e)}"
                )
                logger.error(f"Ingestion error for {uploaded_file.name}: {str(e)}")
                continue
            except Exception as e:
                status_container.error(
                    f"Unexpected error with {uploaded_file.name}: {str(e)}"
                )
                logger.exception(f"Unexpected error processing {uploaded_file.name}")
                continue
            finally:
                # Clear progress indicators for next file
                progress_container.empty()
                status_container.empty()

        # Final success message
        st.success("All files processed!")

    # Add helpful information in sidebar
    with st.sidebar:
        st.header("Information")
        st.markdown(
            """
        ### Supported Features
        - Multiple file upload
        - Excel formats: .xlsx, .xls
        - Data preview before ingestion
        - Progress tracking
        - Batch processing
        - Original filename preservation

        ### Data Processing
        - Files are stored in bronze layer
        - Each row is converted to JSON
        - Metadata is preserved
        - Automatic data type handling
        """
        )


if __name__ == "__main__":
    main()
