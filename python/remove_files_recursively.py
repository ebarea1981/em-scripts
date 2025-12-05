#!/usr/bin/env python3

import os
import re
import argparse

def remove_files_recursively(folder, pattern):
    compiled_pattern = re.compile(pattern)
    for root, _, files in os.walk(folder):
        for filename in files:
            if compiled_pattern.match(filename):
                file_path = os.path.join(root, filename)
                try:
                    os.remove(file_path)
                    print(f"Removed: {file_path}")
                except Exception as e:
                    print(f"Error removing {file_path}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove files with a specific pattern recursively from a folder.")
    parser.add_argument("--folder", required=True, help="Path to the folder to scan.")
    parser.add_argument("--suffix", default="duplicates_marked.bam*", help="File name suffix to remove (default: duplicates_marked.bam*).")
    args = parser.parse_args()

    # Regex pattern for files ending with the specified suffix
    file_pattern = rf'^.*{args.suffix}$'
    remove_files_recursively(args.folder, file_pattern)

# Usage examples:
# Make the script executable:
# chmod +x remove_files_recursively.py
# Run the script with a folder path and suffix:
# ./remove_files_recursively.py --folder /path/to/your/folder --suffix .SUCCESS_meth_extraction.txt