#!/bin/python3

import argparse

def extract_bam_filenames(file_path, offset, n, output_file):
    filenames = []

    with open(file_path, 'r') as file:
        # Skip to the offset
        for _ in range(offset):
            next(file, None)

        # Iterate over the next n lines
        for _ in range(n):
            line = file.readline()
            if not line:
                break  # End of file reached
            # Extract BAM filename without extension
            bam_path = line.strip().split('\t')[1]
            bam_filename = bam_path.split('/')[-1].rsplit('.', 1)[0]
            filenames.append(bam_filename)

    # Write the output to a new text file
    with open(output_file, 'w') as out_file:
        for name in filenames:
            out_file.write(f"{name}\n")

    print(f"✅ Filenames saved to \"{output_file}\"")

if __name__ == "__main__":
    # Argument parser for CLI options
    parser = argparse.ArgumentParser(description="Extract BAM filenames without extensions.")
    parser.add_argument('--file_path', type=str, default='md5sum.txt', help='Input file path (default: md5sum.txt)')
    parser.add_argument('--output_file', type=str, default='sample_names.txt', help='Output file path (default: sample_names.txt)')
    parser.add_argument('--offset', type=int, default=0, help='Line offset to start from (default: 0)')
    parser.add_argument('--n', type=int, default=500, help='Number of lines to process (default: 500)')

    args = parser.parse_args()

    extract_bam_filenames(args.file_path, args.offset, args.n, args.output_file)

