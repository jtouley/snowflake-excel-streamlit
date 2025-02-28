import snowflake.connector
import yaml
from login_snowflake_registry import connect_to_snowflake

# Load Snowflake credentials from config.yaml
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)["snowflake"]

    def __init__(self):
        """Initialize with a Snowflake connection."""
        self.connection = connect_to_snowflake()

# Deploy container service
def deploy_snowpark_service():
    conn = connect_to_snowflake()
    cur = conn.cursor()
    cur.execute("CALL SYSTEM$DEPLOY_CONTAINER_SERVICE('snowflake-excel-app', 'snowpark_service.yml');")
    print("Container deployed successfully!")
    cur.close()
    conn.close()

# Run everything in sequence
if __name__ == "__main__":
    login_to_registry()
    deploy_snowpark_service()