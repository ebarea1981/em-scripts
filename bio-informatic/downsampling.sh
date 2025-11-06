#!/bin/bash

# Read samples.csv and process each sample
# Skip the header line and process each row
tail -n +2 samples.csv | while IFS=',' read -r sample_id reads_needed; do
    # Remove any whitespace from the variables
    sample_id=$(echo "$sample_id" | xargs)
    reads_needed=$(echo "$reads_needed" | xargs)
    
    # Run downsampling for both paired-end files
    echo "Processing sample: $sample_id with $reads_needed reads..."
    seqtk sample -s100 "${sample_id}_1.fastq.gz" "$reads_needed" | pigz > "sub_${sample_id}_1.fastq.gz"
    seqtk sample -s100 "${sample_id}_2.fastq.gz" "$reads_needed" | pigz > "sub_${sample_id}_2.fastq.gz"
    echo "Completed sample: $sample_id"
done