#!/bin/bash

# Virtual Environment Path
VENV_PATH="/home/etl-venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python Script
PYTHON_SCRIPT="/home/etl_pipeline.py"

# Get Current Date
current_date=$(date '+%d-%m-%Y')

# Append Current Date in the Log File
log_file="/home/log/etl_$current_date.log"

# Run Python Script and Insert Log
python "$PYTHON_SCRIPT" >> "$log_file" 2>&1