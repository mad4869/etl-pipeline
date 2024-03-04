#!/bin/bash

# Virtual Environment Path
VENV_PATH="/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/etl-venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python Script
PYTHON_SCRIPT="/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/etl_pipeline.py"

# Get Current Date
current_datetime=$(date '+%d-%m-%Y_%H-%M')

# Append Current Date in the Log File
log_file="/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/logs/etl_$current_datetime.log"

# Run Python Script and Insert Log
python "$PYTHON_SCRIPT" >> "$log_file" 2>&1