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
    --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 \
    nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 \
    pbrun fq2bam_meth \
    --ref /genomes/${REFERENCE_FILE} \
    --in-fq /workdir/${INPUT_FASTQ_1} /workdir/${INPUT_FASTQ_1} \
    --out-bam /outputdir/${SAMPLE_ID}.clara_parabrics.duplicates_marked.bam \
    --out-qc-metrics-dir /outputdir/${SAMPLE_ID}.qc-metrics \
    --out-duplicate-metrics /outputdir/${SAMPLE_ID}.deduplicate_metrics.txt \
    --logfile /outputdir/${SAMPLE_ID}.fq2bam_meth_LOG.log \
    --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite