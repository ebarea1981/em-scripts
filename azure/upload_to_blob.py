import os
import argparse
from azure.storage.blob import BlobServiceClient
from pathlib import Path

def upload_file_to_blob(container_client, file_path, blob_name):
    """
    Uploads a file to Azure Blob Storage.
    
    Args:
        container_client: The Azure Blob Storage container client.
        file_path: Local path to the file to be uploaded.
        blob_name: The blob name to store in Azure.
    """
    try:
        with open(file_path, 'rb') as data:
            container_client.upload_blob(name=blob_name, data=data)
        print(f"Uploaded {file_path} to {blob_name}")
    except Exception as e:
        print(f"Failed to upload {file_path}: {str(e)}")

def upload_folder_to_blob(container_client, folder_path, base_blob_path=""):
    """
    Recursively uploads a folder to Azure Blob Storage.

    Args:
        container_client: The Azure Blob Storage container client.
        folder_path: The local folder path to be uploaded.
        base_blob_path: The blob directory path in the container (defaults to root).
    """
    folder_path = Path(folder_path)
    for root, _, files in os.walk(folder_path):
        for file in files:
            print(f"copying file: {file}")
            file_path = Path(root) / file
            relative_path = file_path.relative_to(folder_path)
            blob_name = os.path.join(base_blob_path, relative_path.as_posix())
            upload_file_to_blob(container_client, file_path, blob_name)

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Upload a folder to Azure Blob Storage recursively.")
    parser.add_argument('--folder', type=str, help="The path to the folder to upload.")
    parser.add_argument('--container', type=str, help="The name of the Azure Blob Storage container.")
    parser.add_argument('--container_path', type=str, help="The name of the Azure Blob Storage container.")
    
    args = parser.parse_args()
    
    # Retrieve connection string from environment variable
    connection_string = "MY_CONNECTION_STRING"
    if not connection_string:
        print("Error: AZURE_STORAGE_CONNECTION_STRING environment variable not set.")
        return
    
    # Create the BlobServiceClient and ContainerClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(args.container)
    
    # Check if the container exists or create it
    try:
        container_client.get_container_properties()
    except Exception:
        print(f"Container {args.container} does not exist. Creating it...")
        container_client.create_container()
    
    # Upload folder to Azure Blob Storage
    print("OK")
    upload_folder_to_blob(container_client, args.folder, base_blob_path=args.container_path)

if __name__ == "__main__":
    main()

