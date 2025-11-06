#!/bin/bash

# Configuration
INPUT_FILE="md5sum.txt"
BASE_OUTPUT_DIR="/mnt/d/samples"
STATE_FILE="download_state_bams.json"
PROJECT_ID="HRA006113"
MAX_PARALLEL=5
LOG_FILE="download_log.txt"

# SFTP Configuration
SFTP_HOST="human.big.ac.cn"
SFTP_USERNAME="my@mail.com"
SFTP_PASSWORD="password"

# Function to log messages
log_message() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "$timestamp - $1" | tee -a "$LOG_FILE"
}

# Function to calculate MD5
calculate_md5() {
    local file="$1"
    if [[ -f "$file" ]]; then
        md5sum "$file" | cut -d' ' -f1
    fi
}

# Function to verify MD5
verify_md5() {
    local file="$1"
    local expected_md5="$2"
    
    if [[ ! -f "$file" ]]; then
        return 1
    fi
    
    local calculated_md5=$(calculate_md5 "$file")
    if [[ "$calculated_md5" == "$expected_md5" ]]; then
        return 0
    else
        return 1
    fi
}

# Function to extract run ID from path
extract_run_id() {
    local path="$1"
    echo "$path" | cut -d'/' -f3
}

# Function to update state file
update_state() {
    local file_name="$1"
    local status="$2"
    local md5="$3"
    local path="$4"
    local error="$5"
    local timestamp=$(date -Iseconds)
    
    # Create state entry
    local state_entry
    if [[ "$status" == "completed" ]]; then
        state_entry="{\"status\":\"$status\",\"timestamp\":\"$timestamp\",\"md5\":\"$md5\",\"path\":\"$path\"}"
    else
        state_entry="{\"status\":\"$status\",\"timestamp\":\"$timestamp\",\"error\":\"$error\"}"
    fi
    
    # Update state file
    if [[ -f "$STATE_FILE" ]]; then
        # Use temporary file for atomic update
        local tmp_file="${STATE_FILE}.tmp"
        jq --arg fn "$file_name" --argjson entry "$state_entry" \
           '. + {($fn): $entry}' "$STATE_FILE" > "$tmp_file" && \
        mv "$tmp_file" "$STATE_FILE"
    else
        # Create new state file
        echo "{\"$file_name\": $state_entry}" > "$STATE_FILE"
    fi
}

# Function to download a single file
download_file() {
    local md5sum="$1"
    local file_path="$2"
    local file_name=$(basename "$file_path")
    local run_id=$(extract_run_id "$file_path")
    local output_dir="${BASE_OUTPUT_DIR}/${PROJECT_ID}/${run_id}"
    local output_path="${output_dir}/${file_name}"
    
    # Create output directory
    mkdir -p "$output_dir"
    
    # Check if file exists and verify MD5
    if [[ -f "$output_path" ]]; then
        if verify_md5 "$output_path" "$md5sum"; then
            log_message "File $file_name already exists and is verified"
            update_state "$file_name" "completed" "$md5sum" "$output_path"
            return 0
        else
            local file_size=$(stat -f%z "$output_path" 2>/dev/null || stat -c%s "$output_path")
            log_message "Partial download found for $file_name ($file_size bytes). Resuming..."
        fi
    fi
    
    # Construct and execute lftp command
    local lftp_cmd="get -c \"$file_path\" -o \"$output_path\""

    log_message "Starting download for $file_name to $output_dir"
    
    if lftp -u "$SFTP_USERNAME,$SFTP_PASSWORD" "sftp://$SFTP_HOST" -e "$lftp_cmd; bye" 2>/tmp/lftp_error.$$; then
        if verify_md5 "$output_path" "$md5sum"; then
            log_message "Successfully downloaded and verified $file_name"
            update_state "$file_name" "completed" "$md5sum" "$output_path"
            rm -f /tmp/lftp_error.$$
            return 0
        else
            local error_msg="MD5 verification failed"
            log_message "ERROR: $error_msg for $file_name"
            update_state "$file_name" "failed" "" "" "$error_msg"
            rm -f /tmp/lftp_error.$$
            return 1
        fi
    else
        local error_msg=$(cat /tmp/lftp_error.$$)
        log_message "ERROR: Failed to download $file_name: $error_msg"
        update_state "$file_name" "failed" "" "" "$error_msg"
        rm -f /tmp/lftp_error.$$
        return 1
    fi
}

# Main execution
main() {
    # Initialize log file
    : > "$LOG_FILE"
    
    # Create base output directory
    mkdir -p "$BASE_OUTPUT_DIR"
    
    # Create empty state file if it doesn't exist
    [[ ! -f "$STATE_FILE" ]] && echo "{}" > "$STATE_FILE"
    
    # Process input file
    if [[ ! -f "$INPUT_FILE" ]]; then
        log_message "ERROR: Input file $INPUT_FILE not found"
        exit 1
    fi
    
    # Create temporary directory for process tracking
    TEMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TEMP_DIR"' EXIT
    
    # Start downloads in parallel
    while read -r md5sum filepath || [[ -n "$filepath" ]]; do
        [[ -z "$md5sum" || -z "$filepath" ]] && continue
        
        # Wait if we have too many parallel jobs
        while [[ $(jobs -p | wc -l) -ge $MAX_PARALLEL ]]; do
            sleep 1
        done
        
        # Start download in background
        (
            download_file "$md5sum" "$filepath"
            echo $? > "$TEMP_DIR/$(basename "$filepath").exit"
        ) &
        
    done < "$INPUT_FILE"
    
    # Wait for all background jobs to complete
    wait
    
    # Count successes and failures
    successful=0
    failed=0
    for exit_file in "$TEMP_DIR"/*.exit; do
        [[ -f "$exit_file" ]] || continue
        if [[ $(cat "$exit_file") -eq 0 ]]; then
            ((successful++))
        else
            ((failed++))
        fi
    done
    
    log_message "Download complete. Successfully downloaded: $successful, Failed: $failed"
}

# Run main function
main 