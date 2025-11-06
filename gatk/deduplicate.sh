#!/usr/bin/bash

docker run -it --rm --volume D:\samples\canola\R023_WT_3:/workdir broadinstitute/gatk samtools view -b -h -F 0x400 -o /workdir/ded/R023_WT_3.clara_parabrics.duplicated.L6.bam /workdir/R023_WT_3.clara_parabrics.duplicates_marked.L6.bam


# gatk MarkDuplicates \
#   -I input.bam \
#   -O output_dedup.bam \
#   -M output_metrics.txt \
#   --REMOVE_DUPLICATES true
