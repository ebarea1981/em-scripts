#!/bin/bash

WORKING_DIR=/mnt/d/samples/arabidopsis/hardik/all_samples
OUTPUT_DIR=${WORKING_DIR}/out
GENOMES_DIR=/mnt/d/genomes/arabidopsis_thaliana
REFERENCE_FILE=Arabidopsis_thaliana.TAIR10.dna.toplevel.fa

SAMPLE_ID=COL_M2
INPUT_FASTQ_1="COL_M2_1.fq.gz"
INPUT_FASTQ_2="COL_M2_2.fq.gz"


docker run --gpus all --rm \
    --volume ${WORKING_DIR}:/workdir \
    --volume ${OUTPUT_DIR}:/outputdir \
    --volume ${GENOMES_DIR}:/genomes \
    --workdir /workdir \
    ebarea1981/methyldackel:latest \
    MethylDackel extract \
      --CHG \
      --CHH \
      --cytosine_report \
      /genomes/${REFERENCE_FILE} \
      /outputdir/${SAMPLE_ID}.clara_parabrics.duplicates_marked.bam \
      -@ 16 \
      -o ${SAMPLE_ID}