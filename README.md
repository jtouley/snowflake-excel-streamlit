# Snowflake Excel to Iceberg Bronze Layer

## Overview
A lightweight, scalable solution for ingesting Excel files into Snowflake using Iceberg tables and providing a user-friendly Streamlit interface. Built for rapid data ingestion without complex DAG architecture.

### Key Features
- 🚀 Streamlit web interface for easy file uploads
- 📊 Automatic data type handling and JSON conversion
- 💾 Efficient batch processing with connection pooling
- 🏗️ Bronze layer storage using Iceberg tables
- 📝 Metadata preservation for downstream processing

## Getting Started

### Prerequisites
- Python 3.11+
- Snowflake account with appropriate permissions
- Git (for version control)

### Installation

1. Clone the repository:
```bash
git clone [your-repo-url]
cd snowflake-excel-streamlit
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Configuration

1. Create `config/config.yaml`:
```yaml
snowflake:
  account: "your_account"
  user: "your_username"
  password: "your_password"
  warehouse: "your_warehouse"
  database: "your_database"
  schema: "your_schema"
```

2. Create Snowflake Iceberg table:
```sql
CREATE OR REPLACE ICEBERG TABLE bronze_table (
    id STRING,
    filename STRING,
    uploaded_at TIMESTAMP_NTZ,
    raw_data STRING
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'iceberg_external_volume'
    BASE_LOCATION = 'bronze';
```

## Usage

### Running the Streamlit Interface
```bash
streamlit run app.py
```

### Command Line Usage
```bash
python scripts/ingest_pooled.py your_file.xlsx
```

## Project Structure
```
.
├── app.py                 # Streamlit application
├── config/
│   └── config.yaml       # Snowflake configuration
├── scripts/
│   ├── __init__.py
│   ├── bronze_ingestor.py # Main ingestor interface
│   └── ingest_pooled.py  # Core ingestion logic
└── requirements.txt      # Project dependencies
```

## Data Architecture

### Bronze Layer Design
- Raw data preservation
- JSON structure with metadata
- Automatic type handling
- Batch processing support

### JSON Structure
```json
{
    "metadata": {
        "column_names": [...],
        "dtypes": {...}
    },
    "data": {
        "column1": "value1",
        ...
    }
}
```

## Performance Considerations
- Batch processing (10,000 rows per batch)
- Connection pooling
- Efficient serialization
- Proper cleanup of resources

## Development


### Adding New Features
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## Security
- Credentials stored in config file (not in version control)
- Environment variable support
- Proper connection handling
- Automatic resource cleanup

## Limitations
- Supports .xlsx and .xls files only
- Single table bronze layer
- No real-time processing
- Limited to Snowflake Iceberg tables

## Future Enhancements
- [ ] Schema validation
- [ ] Silver layer processing
- [ ] Real-time monitoring
- [ ] Data quality checks
- [ ] Multi-table support

## Contributing
Contributions welcome! Please read contributing.md for details.

## Support
For support, please jason.touleyrou@jtouley.com
