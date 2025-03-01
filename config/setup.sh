#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up Excel to Bronze Pipeline Project...${NC}"

# Check if venv directory exists
if [ ! -d "venv" ]; then
    echo -e "${GREEN}Creating Python virtual environment...${NC}"
    python3 -m venv venv
else
    echo -e "${YELLOW}Virtual environment already exists. Continuing...${NC}"
fi

echo -e "${GREEN}Activating virtual environment...${NC}"
source venv/bin/activate

echo -e "${GREEN}Upgrading pip...${NC}"
pip install --upgrade pip

echo -e "${GREEN}Installing dependencies...${NC}"
pip install -r requirements.txt

echo -e "${GREEN}Installing package in development mode...${NC}"
pip install -e .

echo -e "${GREEN}Setting up pre-commit hooks...${NC}"
pre-commit install

# Create config directory if it doesn't exist
if [ ! -d "config" ]; then
    echo -e "${GREEN}Creating config directory...${NC}"
    mkdir -p config
fi

# Create example config file if it doesn't exist
if [ ! -f "config/config.yaml" ]; then
    echo -e "${GREEN}Creating example config file...${NC}"
    cat > config/config.yaml << EOL
snowflake:
  account: "your_account"
  user: "your_username"
  password: "your_password"
  warehouse: "your_warehouse"
  database: "your_database"
  schema: "your_schema"

application:
  log_level: "INFO"
  batch_size: 10000
  bronze_table: "bronze_table"
EOL
    echo -e "${YELLOW}Please update the config/config.yaml file with your Snowflake credentials${NC}"
fi

echo -e "${GREEN}Running pre-commit checks on all files...${NC}"
pre-commit run --all-files

echo -e "${GREEN}Setup complete!${NC}"
echo -e "${YELLOW}To start working, activate the environment:${NC}"
echo -e "    source venv/bin/activate"
echo -e "${YELLOW}To run the Streamlit app:${NC}"
echo -e "    streamlit run app.py"
echo -e "${YELLOW}To run the CLI tool:${NC}"
echo -e "    python -m excel_to_bronze your_file.xlsx"
echo -e "${YELLOW}For more information, see the README.md${NC}"
