import snowflake.connector
import yaml

# Load Snowflake credentials from config
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)["snowflake"]

def connect_to_snowflake():
    """Establish a Snowflake connection."""
    return snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["account"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"]
    )

def create_iceberg_table():
    """Creates the Iceberg table if it doesn't exist."""
    conn = connect_to_snowflake()
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE OR REPLACE ICEBERG TABLE bronze_table (
        id STRING,
        filename STRING,
        uploaded_at TIMESTAMP_NTZ,  -- Removed DEFAULT CURRENT_TIMESTAMP()
        raw_data STRING  -- Store JSON as a string
    )
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = 'iceberg_external_volume'
        BASE_LOCATION = 'bronze'
    """
    
    cursor.execute(create_table_query)
    print("✅ Iceberg table created successfully.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_iceberg_table()