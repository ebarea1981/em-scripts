#!/usr/bin/bash

docker run -it --gpus all --rm \
    --volume /data1/tests/bam:/workdir \
    --volume /home/ebarea/0_trabajo/0_tests_remove_later/out:/outputdir \
    --volume /data1/genomes/arabidopsis_thaliana:/genomes \
    nvcr.io/nvidia/clara/clara-parabricks:4.3.1-1 \
    bash
    # pbrun bammetrics --ref /genomes/Arabidopsis_thaliana.TAIR10.dna.toplevel.fa \
    # --bam /workdir/cas_R12_1.methyldackel_marked_dedup.bam \
    # --out-metrics-file /outputdir/metrics.txt

docker run --rm --gpus all nvidia/cuda:12.6.0-base-ubuntu24.04 nvidia-smi

docker run -it --gpus all --rm nvcr.io/nvidia/clara/clara-parabricks:4.2.0-1 bash

docker run -it --gpus all --rm nvcr.io/nvidia/clara/clara-parabricks:4.3.1-1 bash