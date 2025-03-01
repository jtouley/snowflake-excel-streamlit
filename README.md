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

2. Run the setup.sh:
```bash
bash config/setup.sh
```
### This script will:
- Create and activate a Python virtual environment (if not already present)
- Upgrade pip and install all dependencies from requirements.txt
- Install the package in development mode
- Set up pre-commit hooks
- Create the config directory and an example configuration file if they do not exist

### Configuration

1. Update the `config/config.yaml`:
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
python -m excel_to_bronze sample.xlsx
```

## Project Structure
```
.
├── README.md                            # Project documentation
├── app.py                               # Main application entry point
├── config
│   ├── config.yaml                      # Your Snowflake and application configuration (ignored by Git)
│   ├── config.yaml.example              # Example configuration file
│   └── setup.sh                         # Setup script for environment and dependencies
├── excel_to_bronze                      # Core package for data ingestion and processing
│   ├── __init__.py
│   ├── __main__.py                      # Module entry point for execution
│   ├── config.py                        # Configuration manager for the package
│   ├── connectors                       # External connectors (e.g., Snowflake)
│   │   ├── __init__.py
│   │   └── snowflake.py
│   ├── ingestion                        # Modules for data ingestion
│   │   ├── __init__.py
│   │   ├── base.py                      # Base ingestion classes and error definitions
│   │   ├── bronze.py                    # Bronze layer ingestion implementation
│   │   └── serializers.py               # Data serialization utilities
│   └── utils                            # Utility modules (e.g., logging)
│       ├── __init__.py
│       └── logging.py
├── excel_to_bronze.egg-info             # Packaging metadata (auto-generated)
├── pyproject.toml                       # Build and tool configuration
├── requirements.txt                     # Project dependencies
├── sample.xlsx                          # Sample Excel file for testing
└── setup.py                             # Installation and setup script
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
    "column_names": ["col1", "col2", "col3"],
    "dtypes": {"col1": "int", "col2": "string", "col3": "float"}
  },
  "data": {
    "col1": 1,
    "col2": "value",
    "col3": 3.14
  }
}
```

## Performance Considerations
- Batch processing (10,000 rows per batch)
- Connection pooling
- Efficient serialization
- Proper cleanup of resources

## Development

### Code Quality & Best Practices
- Refactored for Maintainability: Adheres to DRY and SOLID principles.
- Linting & Pre-commit Hooks: Ensures code consistency via tools defined in pyproject.toml and the pre-commit configuration.
- Testing: Includes unit and integration tests to ensure robustness.


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
