#!/bin/bash

# ======================
# CONFIGURATION
# ======================
sample_file="sample_names.txt"  # Path to your sample names file
ftp_server="sftp://fms.biosino.org"  # Changed to SFTP and correct hostname
username="myusername@domain.com"
password="mypassword"
local_base="/mnt/d/samples/HRA006113"      # Local base directory for downloads

# ======================
# Check SFTP connection
# ======================
if ! lftp -u "$username","$password" "$ftp_server" -e "ls; bye" &>/dev/null; then
    echo "❌ Error: Cannot connect to SFTP server. Please check your credentials and server address."
    exit 1
fi

# ======================
# MAIN SCRIPT
# ======================
while IFS= read -r sample_id; do
    if [ -n "$sample_id" ]; then
        # Build the remote and local paths
        remote_path="HRA006113/$sample_id"
        local_dest="$local_base/$sample_id"

        echo "🔄 Downloading $remote_path to $local_dest"

        # Create local directory if it doesn't exist
        mkdir -p "$local_dest"

        # lftp command to mirror the folder with error handling
        if ! lftp -u "$username","$password" "$ftp_server" -e "set sftp:auto-confirm yes; mirror -c $remote_path $local_dest; bye"; then
            echo "❌ Error downloading $remote_path"
            continue
        fi
        
        echo "✅ Successfully downloaded $sample_id"
    fi
done < "$sample_file"

echo "✅ Download completed." 