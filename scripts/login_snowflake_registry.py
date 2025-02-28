import snowflake.connector
import yaml

# Load Snowflake credentials from config.yaml
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)["snowflake"]

def connect_to_snowflake():
    """Establish a Snowflake connection using credentials from config.yaml."""
    return snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["account"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"]
    )

# Connect to Snowflake
conn = connect_to_snowflake()
cur = conn.cursor()

# Call the registry login procedure
cur.execute("CALL SYSTEM$REGISTRY_LOGIN();")

# Fetch and print the Docker login command
login_cmd = cur.fetchone()[0]
print("\nRun the following command in your terminal:\n")
print(login_cmd)

# Close connection
cur.close()
conn.close()