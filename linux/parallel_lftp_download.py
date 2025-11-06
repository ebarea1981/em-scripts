#!/usr/bin/env python

import csv
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime
import json
import hashlib

# Configuration
INPUT_FILE = "azure/your_file.txt"
MAX_WORKERS = 5  # Number of parallel downloads
BASE_OUTPUT_DIR = "/mnt/d/samples"  # Base directory for downloads
STATE_FILE = "download_state.json"

# SFTP Configuration
SFTP_HOST = "fms.biosino.org"
SFTP_PORT = 44398
SFTP_USERNAME = "my@mail.com"
SFTP_PASSWORD = "password"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_log.txt'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def load_state():
    """Load the state from STATE_FILE."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    """Save the current state to STATE_FILE."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def calculate_md5(file_path):
    """Calculate MD5 hash of a file."""
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def create_lftp_command(row):
    """Create lftp command for a given row from the input file."""
    project_id = row['project_id']
    run_id = row['run_id']
    ftp_path = row['ftp_file_path']
    
    # Create output directory path
    output_dir = f"{BASE_OUTPUT_DIR}/{project_id}/{run_id}"
    
    # Construct the lftp command with enhanced resume capability
    command = f"""set net:reconnect-interval-base 10 && \
set net:max-retries 5 && \
set net:timeout 300 && \
set net:connection-limit 5 && \
set net:connection-takeover yes && \
set mirror:parallel-transfer-count 5 && \
set xfer:clobber off && \
set xfer:log yes && \
mkdir -p {output_dir} && \
cd {output_dir} && \
lftp -p {SFTP_PORT} -u {SFTP_USERNAME},{SFTP_PASSWORD} sftp://{SFTP_HOST} -e 'set xfer:clobber off; get -c "{ftp_path}"; bye'"""
    
    return command, output_dir

def verify_download(file_path, expected_md5):
    """Verify if file exists and has correct MD5."""
    if not os.path.exists(file_path):
        return False
    
    calculated_md5 = calculate_md5(file_path)
    return calculated_md5.lower() == expected_md5.lower()

def download_file(row, state):
    """Execute the lftp command for a single file and verify MD5."""
    file_name = row['fileName']
    expected_md5 = row['MD5']
    project_id = row['project_id']
    run_id = row['run_id']
    
    output_path = f"{BASE_OUTPUT_DIR}/{project_id}/{run_id}/{file_name}"
    
    # Check for partial downloads
    if os.path.exists(output_path):
        if verify_download(output_path, expected_md5):
            logger.info(f"File {file_name} already exists and is verified")
            state[file_name] = {
                'status': 'completed',
                'timestamp': str(datetime.now()),
                'md5': expected_md5,
                'path': output_path
            }
            save_state(state)
            return True
        else:
            file_size = os.path.getsize(output_path)
            logger.info(f"Partial download found for {file_name} ({file_size} bytes). Resuming...")
    
    try:
        command, output_dir = create_lftp_command(row)
        
        # Log the download attempt
        logger.info(f"Starting download for {file_name} to {output_dir}")
        
        # Execute the command
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode == 0:
            # Verify the downloaded file
            if verify_download(output_path, expected_md5):
                logger.info(f"Successfully downloaded and verified {file_name}")
                state[file_name] = {
                    'status': 'completed',
                    'timestamp': str(datetime.now()),
                    'md5': expected_md5,
                    'path': output_path
                }
                save_state(state)
                return True
            else:
                logger.error(f"MD5 verification failed for {file_name}")
                state[file_name] = {
                    'status': 'failed',
                    'timestamp': str(datetime.now()),
                    'error': 'MD5 verification failed'
                }
                save_state(state)
                return False
        else:
            logger.error(f"Failed to download {file_name}: {result.stderr}")
            state[file_name] = {
                'status': 'failed',
                'timestamp': str(datetime.now()),
                'error': result.stderr
            }
            save_state(state)
            return False
            
    except Exception as e:
        logger.error(f"Error processing {file_name}: {str(e)}")
        state[file_name] = {
            'status': 'failed',
            'timestamp': str(datetime.now()),
            'error': str(e)
        }
        save_state(state)
        return False

def main():
    # Create base output directory if it doesn't exist
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    
    # Load state
    state = load_state()
    
    # Read the input file
    try:
        with open(INPUT_FILE, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            rows = list(reader)
    except Exception as e:
        logger.error(f"Error reading input file: {str(e)}")
        return

    # Filter out successfully completed downloads
    pending_rows = [
        row for row in rows 
        if row['fileName'] not in state or 
        state[row['fileName']].get('status') != 'completed' or
        not verify_download(
            f"{BASE_OUTPUT_DIR}/{row['project_id']}/{row['run_id']}/{row['fileName']}", 
            row['MD5']
        )
    ]

    if not pending_rows:
        logger.info("No files to download - all files are already downloaded and verified")
        return

    logger.info(f"Starting parallel download of {len(pending_rows)} files with {MAX_WORKERS} workers")
    
    # Process files in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_file, row, state) for row in pending_rows]
        results = [future.result() for future in futures]
    
    # Summary
    successful = sum(1 for r in results if r)
    failed = len(results) - successful
    logger.info(f"Download complete. Successfully downloaded: {successful}, Failed: {failed}")

if __name__ == "__main__":
    main() 