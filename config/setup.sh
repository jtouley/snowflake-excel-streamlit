#!/bin/bash

echo "Setting up the Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

echo "Installing dependencies..."
pip install -r requirements.txt

echo "Installing linting tools (black, flake8)..."
pip install black flake8

echo "Setup complete. Run 'source venv/bin/activate' to start coding!"