#!/bin/env python

import csv
import pysftp
from azure.storage.blob import BlobServiceClient
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime
from azure.core.exceptions import ResourceNotFoundError

# Configuration
SFTP_HOST = "fms.biosino.org"  # removed sftp:// prefix as pysftp doesn't need it
SFTP_PORT = 44398
SFTP_USERNAME = "my@mail.com"
SFTP_PASSWORD = "password"
AZURE_STORAGE_CONNECTION_STRING = "MY_CONNECTION_STRING"
AZURE_CONTAINER_NAME = "emresearch"
INPUT_FILE = "azure/your_file.txt"
STATE_FILE = "download_state.json"
MAX_WORKERS = 20  # Adjust based on SFTP server limits and network
CHUNK_SIZE = 10 * 1024 * 1024  # 4 MB chunks

# Disable host key checking (only if you trust the host)
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

# Initialize Azure Blob Service Client and ensure container exists
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
try:
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    container_client.get_container_properties()
except ResourceNotFoundError:
    logger.info(f"Container {AZURE_CONTAINER_NAME} not found. Creating it...")
    container_client = blob_service_client.create_container(AZURE_CONTAINER_NAME)

# Load or initialize state file
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

# Check if file exists in Blob Storage with correct MD5
def check_blob_exists(file_name, expected_md5):
    try:
        blob_client = container_client.get_blob_client(file_name)
        properties = blob_client.get_blob_properties()
        blob_md5 = properties.content_settings.content_md5.hex() if properties.content_settings.content_md5 else None
        return blob_md5 == expected_md5
    except ResourceNotFoundError:
        return False
    except Exception as e:
        logger.warning(f"Error checking blob {file_name}: {str(e)}")
        return False

# Process a single file with streaming
def process_file(row, state):
    ftp_path = row["ftp_file_path"]
    expected_md5 = row["MD5"]
    file_name = row["fileName"]

    # Skip if already completed or exists in Blob with correct MD5
    if state.get(file_name, {}).get("status") == "completed" or check_blob_exists(file_name, expected_md5):
        logger.info(f"Skipping {file_name} - already completed or verified in Blob Storage")
        return True

    try:
        # SFTP connection (pysftp handles transport internally)
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # Disable host key checking (use with caution; see notes)
        with pysftp.Connection(
            SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USERNAME,
            password=SFTP_PASSWORD,
            cnopts=cnopts
        ) as sftp:
            # Azure Blob client
            blob_client = container_client.get_blob_client(file_name)

            # Stream file and calculate MD5
            md5_hash = hashlib.md5()
            with sftp.open(ftp_path, "rb") as remote_file:
                block_list = []
                block_id = 0

                while True:
                    chunk = remote_file.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    md5_hash.update(chunk)
                    block_id += 1
                    block_id_str = f"{block_id:06d}"
                    blob_client.stage_block(block_id_str, chunk)
                    block_list.append(block_id_str)

                if not block_list:
                    logger.error(f"No data read from {file_name}")
                    state[file_name] = {"ftp_path": ftp_path, "md5": expected_md5, "status": "failed", "timestamp": str(datetime.now())}
                    save_state(state)
                    return False

                calculated_md5 = md5_hash.hexdigest()
                if calculated_md5 != expected_md5:
                    logger.error(f"MD5 mismatch for {file_name}: expected {expected_md5}, got {calculated_md5}")
                    state[file_name] = {"ftp_path": ftp_path, "md5": expected_md5, "status": "failed", "timestamp": str(datetime.now())}
                    save_state(state)
                    return False

                # Commit the blob
                blob_client.commit_block_list(block_list)
                blob_client.set_blob_properties(content_settings={"content_md5": bytes.fromhex(calculated_md5)})
                state[file_name] = {"ftp_path": ftp_path, "md5": expected_md5, "status": "completed", "timestamp": str(datetime.now())}
                save_state(state)
                logger.info(f"Uploaded {file_name} with MD5 {calculated_md5}")
                return True

    except Exception as e:
        logger.error(f"Failed to process {file_name}: {str(e)}")
        state[file_name] = {"ftp_path": ftp_path, "md5": expected_md5, "status": "failed", "timestamp": str(datetime.now())}
        save_state(state)
        return False

# Main execution
def main():
    # Load state and input file
    state = load_state()
    with open(INPUT_FILE, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = [row for row in reader if row["fileName"] not in state or state[row["fileName"]]["status"] != "completed"]

    if not rows:
        logger.info("All files already completed or verified. Nothing to do.")
        return

    logger.info(f"Starting/resuming download of {len(rows)} remaining files with {MAX_WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_row = {executor.submit(process_file, row, state): row for row in rows}
        for future in as_completed(future_to_row):
            row = future_to_row[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Exception for {row['fileName']}: {str(e)}")

    logger.info("Process completed.")

if __name__ == "__main__":
    main()