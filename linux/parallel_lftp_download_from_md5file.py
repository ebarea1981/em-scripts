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
INPUT_FILE = "linux/md5sum.txt"
MAX_WORKERS = 5  # Number of parallel downloads
BASE_OUTPUT_DIR = "/mnt/d/samples"  # Base directory for downloads
STATE_FILE = "download_state_bams.json"
PROJECT_ID = "HRA006113"  # Fixed project ID

# SFTP Configuration
SFTP_HOST = "human.big.ac.cn"
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

def extract_run_id(file_path):
    """Extract run ID from file path (e.g., HRR1458867 from /HRA006113/HRR1458867/...)."""
    parts = file_path.strip('/').split('/')
    if len(parts) >= 2:
        return parts[1]
    return None

def create_lftp_command(file_path, output_dir):
    """Create lftp command for a given file path."""
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
lftp -u {SFTP_USERNAME},{SFTP_PASSWORD} sftp://{SFTP_HOST} -e 'set xfer:clobber off; get -c "{file_path}"; bye'"""
    
    return command

def verify_download(file_path, expected_md5):
    """Verify if file exists and has correct MD5."""
    if not os.path.exists(file_path):
        return False
    
    calculated_md5 = calculate_md5(file_path)
    return calculated_md5.lower() == expected_md5.lower()

def download_file(file_info, state):
    """Execute the lftp command for a single file and verify MD5."""
    md5sum, file_path = file_info
    file_name = os.path.basename(file_path)
    run_id = extract_run_id(file_path)
    
    if not run_id:
        logger.error(f"Could not extract run ID from path: {file_path}")
        return False
    
    # Create output path based on project and run ID
    output_dir = os.path.join(BASE_OUTPUT_DIR, PROJECT_ID, run_id)
    output_path = os.path.join(output_dir, file_name)
    
    # Check for partial downloads
    if os.path.exists(output_path):
        if verify_download(output_path, md5sum):
            logger.info(f"File {file_name} already exists and is verified")
            state[file_name] = {
                'status': 'completed',
                'timestamp': str(datetime.now()),
                'md5': md5sum,
                'path': output_path
            }
            save_state(state)
            return True
        else:
            file_size = os.path.getsize(output_path)
            logger.info(f"Partial download found for {file_name} ({file_size} bytes). Resuming...")
    
    try:
        command = create_lftp_command(file_path, output_dir)
        logger.info(f"Starting download for {file_name} to {output_dir}")
        
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode == 0:
            if verify_download(output_path, md5sum):
                logger.info(f"Successfully downloaded and verified {file_name}")
                state[file_name] = {
                    'status': 'completed',
                    'timestamp': str(datetime.now()),
                    'md5': md5sum,
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

def parse_md5_file(file_path):
    """Parse the MD5 file and return list of (md5sum, file_path) tuples."""
    file_info_list = []
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip():  # Skip empty lines
                try:
                    md5sum, file_path = line.strip().split(None, 1)
                    file_info_list.append((md5sum, file_path))
                except ValueError as e:
                    logger.error(f"Error parsing line: {line.strip()}: {e}")
                    continue
    return file_info_list

def main():
    # Create base output directory if it doesn't exist
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    
    # Load state
    state = load_state()
    
    try:
        # Parse MD5 file
        file_info_list = parse_md5_file(INPUT_FILE)
    except Exception as e:
        logger.error(f"Error reading input file: {str(e)}")
        return

    # Filter out successfully completed downloads
    pending_files = [
        file_info for file_info in file_info_list
        if os.path.basename(file_info[1]) not in state or 
        state[os.path.basename(file_info[1])].get('status') != 'completed' or
        not verify_download(
            os.path.join(BASE_OUTPUT_DIR, PROJECT_ID, extract_run_id(file_info[1]), os.path.basename(file_info[1])),
            file_info[0]
        )
    ]

    if not pending_files:
        logger.info("No files to download - all files are already downloaded and verified")
        return

    logger.info(f"Starting parallel download of {len(pending_files)} files with {MAX_WORKERS} workers")
    
    # Process files in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_file, file_info, state) for file_info in pending_files]
        results = [future.result() for future in futures]
    
    # Summary
    successful = sum(1 for r in results if r)
    failed = len(results) - successful
    logger.info(f"Download complete. Successfully downloaded: {successful}, Failed: {failed}")

if __name__ == "__main__":
    main() 