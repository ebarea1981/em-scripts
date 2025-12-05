#!/usr/bin/env python3

import os
import re
import shutil
import argparse

def move_files_recursively(source_folder, destination_folder, pattern):
    """
    Recursively move files matching the pattern from source to destination,
    preserving the directory structure.

    Args:
        source_folder: Root directory to search for files
        destination_folder: Root directory to move files to
        pattern: Compiled regex pattern to match filenames

    The relative path from source_folder is preserved in destination_folder.
    """
    compiled_pattern = re.compile(pattern)

    # Ensure destination folder exists
    os.makedirs(destination_folder, exist_ok=True)

    for root, _, files in os.walk(source_folder):
        for filename in files:
            if compiled_pattern.match(filename):
                source_path = os.path.join(root, filename)
                # Preserve the relative path structure
                rel_path = os.path.relpath(source_path, source_folder)
                dest_path = os.path.join(destination_folder, rel_path)

                # Ensure destination directory exists
                dest_dir = os.path.dirname(dest_path)
                os.makedirs(dest_dir, exist_ok=True)

                # Handle filename conflicts by adding a suffix
                counter = 1
                original_dest_path = dest_path
                while os.path.exists(dest_path):
                    name, ext = os.path.splitext(filename)
                    new_filename = f"{name}_{counter}{ext}"
                    dest_path = os.path.join(dest_dir, new_filename)
                    counter += 1

                try:
                    shutil.move(source_path, dest_path)
                    print(f"Moved: {source_path} -> {dest_path}")
                except Exception as e:
                    print(f"Error moving {source_path}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move files with a specific pattern recursively from source to destination folder.")
    parser.add_argument("--source", required=True, help="Path to the folder to scan.")
    parser.add_argument("--dest", required=True, help="Path to the folder to move the files to.")
    parser.add_argument("--suffix", default="duplicates_marked.bam*", help="File name suffix pattern to match (default: duplicates_marked.bam*).")
    args = parser.parse_args()

    # Regex pattern for files ending with the specified suffix
    file_pattern = rf'^.*{args.suffix}$'
    move_files_recursively(args.source, args.dest, file_pattern)

# Usage examples:
# Make the script executable:
# chmod +x move_files_recursively.py
# Run the script with source and destination paths:
# ./move_files_recursively.py --source /path/to/source/folder --dest /path/to/destination/folder --suffix .txt
#
# The script preserves the directory structure:
# source/subdir/file.txt -> dest/subdir/file.txt