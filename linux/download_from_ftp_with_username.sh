#!/bin/bash

# ======================
# CONFIGURATION
# ======================
checksum_file="md5sum.txt"   # Path to your checksum file
offset=0                     # Starting line offset (0-based)
n=3                          # Number of lines to process
ftp_server="sftp://fms.biosino.org"  # Changed to SFTP and correct hostname
username="my@mail.com"
password="password"
local_base="/data/eduardo/HRA006113"     # Local base directory for downloads

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
tail -n +"$((offset + 1))" "$checksum_file" | head -n "$n" | while IFS=$'\t' read -r checksum bam_path; do
    # Extract directory from BAM path
    folder_path=$(dirname "$bam_path")
    
    # Build local destination path
    local_dest="$local_base$folder_path"

    echo "🔄 Downloading $folder_path to $local_dest"

    # Create local directory if it doesn't exist
    mkdir -p "$local_dest"

    # lftp command to mirror the folder with error handling
    if ! lftp -u "$username","$password" "$ftp_server" -e "set sftp:auto-confirm yes; mirror -c $folder_path $local_dest; bye"; then
        echo "❌ Error downloading $folder_path"
        continue
    fi

    # Optional: Verify BAM file integrity
    bam_file="$local_dest/$(basename "$bam_path")"
    if [[ -f "$bam_file" ]]; then
        if ! echo "$checksum  $bam_file" | md5sum -c - &>/dev/null; then
            echo "❌ Checksum verification failed for $bam_file"
        else
            echo "✅ Successfully downloaded and verified $bam_file"
        fi
    else
        echo "❌ BAM file not found: $bam_file"
    fi

done

echo "✅ Download and verification completed."
