import streamlit as st
import tempfile
import os
from scripts.bronze_ingestor import BronzeIngestor
import time
import pandas as pd

def main():
    st.set_page_config(
        page_title="Excel to Snowflake Bronze Ingestion",
        page_icon="📊",
        layout="wide"
    )

    st.title("Excel to Snowflake Bronze Layer Ingestion")
    st.markdown("""
    Upload Excel files to be ingested into the Snowflake bronze layer. 
    Files will be processed and stored as JSON in an Iceberg table.
    """)

    # File uploader
    uploaded_files = st.file_uploader(
        "Choose Excel file(s)", 
        type=['xlsx', 'xls'], 
        accept_multiple_files=True
    )

    if uploaded_files:
        ingestor = BronzeIngestor()
        
        # Progress tracking
        progress_container = st.empty()
        status_container = st.empty()
        preview_container = st.empty()

        for uploaded_file in uploaded_files:
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
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_file_path = tmp_file.name

                # Process the file
                ingestor.ingest_excel(tmp_file_path)

                # Update progress
                progress_bar.progress(100)
                status_container.success(f"Successfully processed {uploaded_file.name}")

                # Clean up
                os.unlink(tmp_file_path)
                time.sleep(1)  # Give users time to see the success message

            except Exception as e:
                status_container.error(f"Error processing {uploaded_file.name}: {str(e)}")
                continue

            finally:
                # Clear progress indicators for next file
                progress_container.empty()
                status_container.empty()

        # Final success message
        st.success("All files processed!")

    # Add helpful information
    with st.sidebar:
        st.header("Information")
        st.markdown("""
        ### Supported Features
        - Multiple file upload
        - Excel formats: .xlsx, .xls
        - Data preview before ingestion
        - Progress tracking
        - Batch processing

        ### Data Processing
        - Files are stored in bronze layer
        - Each row is converted to JSON
        - Metadata is preserved
        - Automatic data type handling
        """)

if __name__ == "__main__":
    main()