#!/usr/bin/bash

# print_time docker run -it --gpus all --rm \
    # --volume /data1/samples/canola:/workdir \
    # --volume /data1/samples/canola/out:/outputdir \
    # --volume /data1/genomes/canola:/genomes \
    # nvcr.io/nvidia/clara/clara-parabricks:4.3.1-1 \
    # pbrun fq2bam_meth --low-memory \
    # --in-fq /workdir/R023_WT_1_val_1.fq.gz /workdir/R023_WT_1_val_2.fq.gz \
    # --ref /genomes/BnapusDarmor-bzh_chromosomes.fasta \
    # --out-bam /outputdir/R023_WT_1.clara_parabrics.marked_deduplicate.bam \
    # --out-qc-metrics-dir /outputdir/qc-metrics.R023_WT_1 \
    # --logfile /outputdir/log_R023_WT_1.fq2bam_meth.log


# This command assumes all the inputs are in INPUT_DIR and all the outputs go to OUTPUT_DIR.
# docker run --rm --gpus all --volume INPUT_DIR:/workdir --volume OUTPUT_DIR:/outputdir \
#     --workdir /workdir --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 \
#     nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 (OR nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1.grace) \
#     pbrun fq2bam \
#     --ref /workdir/Homo_sapiens_assembly38.fasta \
#     --in-fq /workdir/fastq1.gz /workdir/fastq2.gz \
#     --out-bam /outputdir/fq2bam_output.bam \
#     --tmp-dir /workdir \
#     --bwa-cpu-thread-pool 16 \
#     --out-recal-file recal.txt \
#     --knownSites /workdir/hg.known_indels.vcf \
#     --gpusort \
#     --gpuwrite

GENOME_REF="BnapusDarmor-bzh_chromosomes.fasta"
SAMPLE_ID="R023_WT_3"

docker run --gpus all --rm --volume D:\backup_old_pc\usftp21.novogene.com\01.RawData\R023_WT_3:/workdir  --volume D:\backup_old_pc\usftp21.novogene.com\01.RawData\R023_WT_3\out:/outputdir --volume D:\genomes\canola:/genomes --workdir /workdir --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 pbrun fq2bam_meth --ref /genomes/BnapusDarmor-bzh_chromosomes.fasta --in-fq /workdir/R023_WT_3_CKDO240000897-1A_22C5V2LT4_L2_1.fq.gz /workdir/R023_WT_3_CKDO240000897-1A_22C5V2LT4_L2_2.fq.gz --out-bam /outputdir/R023_WT_3.clara_parabrics.duplicates_marked.bam --out-qc-metrics-dir /outputdir/qc-metrics.cas_R12_1 --out-duplicate-metrics /outputdir/deduplicate_metrics.R023_WT_3.txt --logfile /outputdir/log_R023_WT_3.fq2bam_meth.log --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite

docker run --gpus all --rm --volume D:\backup_old_pc\usftp21.novogene.com\01.RawData\R023_WT_3:/workdir  --volume D:\backup_old_pc\usftp21.novogene.com\01.RawData\R023_WT_3\out:/outputdir --volume D:\genomes\canola:/genomes --workdir /workdir --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 pbrun fq2bam_meth --ref /genomes/BnapusDarmor-bzh_chromosomes.fasta --in-fq /workdir/R023_WT_3_CKDO240000897-1A_227FT5LT4_L4_1.fq.gz /workdir/R023_WT_3_CKDO240000897-1A_227FT5LT4_L4_2.fq.gz --out-bam /outputdir/R023_WT_3.clara_parabrics.duplicates_marked.L4.bam --out-qc-metrics-dir /outputdir/qc-metrics.cas_R12_1.L4 --out-duplicate-metrics /outputdir/deduplicate_metrics.R023_WT_3.L4.txt --logfile /outputdir/log_R023_WT_3.fq2bam_meth.L4.log --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite

docker run --gpus all --rm --volume D:\backup_old_pc\usftp21.novogene.com\01.RawData\R023_WT_3:/workdir  --volume D:\backup_old_pc\usftp21.novogene.com\01.RawData\R023_WT_3\out:/outputdir --volume D:\genomes\canola:/genomes --workdir /workdir --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 pbrun fq2bam_meth --ref /genomes/BnapusDarmor-bzh_chromosomes.fasta --in-fq /workdir/R023_WT_3_CKDO240000897-1A_22C5VGLT4_L6_1.fq.gz /workdir/R023_WT_3_CKDO240000897-1A_22C5VGLT4_L6_2.fq.gz --out-bam /outputdir/R023_WT_3.clara_parabrics.duplicates_marked.L6.bam --out-qc-metrics-dir /outputdir/qc-metrics.cas_R12_1.L6 --out-duplicate-metrics /outputdir/deduplicate_metrics.R023_WT_3.L6.txt --logfile /outputdir/log_R023_WT_3.fq2bam_meth.L6.log --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite


docker run --gpus all --rm --volume D:\backup_old_pc\usftp21.novogene.com\01.RawData\R023_WT_3:/workdir  --volume D:\backup_old_pc\usftp21.novogene.com\01.RawData\R023_WT_3\out_2in1:/outputdir --volume D:\genomes\canola:/genomes --workdir /workdir --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 pbrun fq2bam_meth --ref /genomes/BnapusDarmor-bzh_chromosomes.fasta --in-fq /workdir/R023_WT_3_CKDO240000897-1A_22C5V2LT4_L2_1.fq.gz /workdir/R023_WT_3_CKDO240000897-1A_22C5V2LT4_L2_2.fq.gz --in-fq /workdir/R023_WT_3_CKDO240000897-1A_22C5VGLT4_L6_1.fq.gz /workdir/R023_WT_3_CKDO240000897-1A_22C5VGLT4_L6_2.fq.gz --out-bam /outputdir/R023_WT_3.clara_parabrics.duplicates_marked.2in1.bam --out-qc-metrics-dir /outputdir/qc-metrics.cas_R12_1 --out-duplicate-metrics /outputdir/deduplicate_metrics.R023_WT_3.txt --logfile /outputdir/log_R023_WT_3.fq2bam_meth.log --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite



docker run --gpus all --rm \
    --volume /mnt/rawdata/R016_WT_1:/workdir \
    --volume /mnt/rawdata/R016_WT_1/out_align:/outputdir \
    --volume /mnt/rawdata/canola:/genomes \
    --workdir /workdir \
    --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 \
    nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 pbrun fq2bam_meth \
    --ref /genomes/BnapusDarmor-bzh_chromosomes.fasta \
    --in-fq /workdir/R016_WT_1_CKDO240000880-1A_227FT5LT4_L2_1.fq.gz /workdir/R016_WT_1_CKDO240000880-1A_227FT5LT4_L2_2.fq.gz \
    --in-fq /workdir/R016_WT_1_CKDO240000880-1A_227FT5LT4_L3_1.fq.gz /workdir/R016_WT_1_CKDO240000880-1A_227FT5LT4_L3_2.fq.gz \
    --in-fq /workdir/R016_WT_1_CKDO240000880-1A_227FT5LT4_L4_1.fq.gz /workdir/R016_WT_1_CKDO240000880-1A_227FT5LT4_L4_2.fq.gz \
    --in-fq /workdir/R016_WT_1_CKDO240000880-1A_22C5VGLT4_L2_1.fq.gz /workdir/R016_WT_1_CKDO240000880-1A_22C5VGLT4_L2_2.fq.gz \
    --out-bam /outputdir/R016_WT_1.clara_parabrics.duplicates_marked.bam \
    --out-qc-metrics-dir /outputdir/qc-metrics.cas_R12_1 \
    --out-duplicate-metrics /outputdir/deduplicate_metrics.R016_WT_1.txt \
    --logfile /outputdir/log_R016_WT_1.fq2bam_meth.log \
    --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite --low-memory


docker run --gpus all --rm \
    --volume /mnt/rawdata/arabidopsis:/workdir \
    --volume /mnt/rawdata/arabidopsis/out_align:/outputdir \
    --volume /mnt/rawdata/genomes/arabidopsis_thaliana:/genomes \
    --workdir /workdir \
    --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 \
    nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 pbrun fq2bam_meth \
    --ref /genomes/Arabidopsis_thaliana.TAIR10.dna.toplevel.fa \
    --in-fq /workdir/cas_R12_1_1.fq.gz /workdir/cas_R12_1_2.fq.gz \
    --out-bam /outputdir/cas_R12_1.clara_parabrics.duplicates_marked.bam \
    --out-qc-metrics-dir /outputdir/qc-metrics.cas_R12_1 \
    --out-duplicate-metrics /outputdir/deduplicate_metrics.cas_R12_1.txt \
    --logfile /outputdir/log_cas_R12_1.fq2bam_meth.log \
    --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite --low-memory


docker run --rm \
    --volume /data2/eduardo/test_canola/R016_WT_1:/workdir \
    --volume /data2/eduardo/test_canola/R016_WT_1/out_ded:/outputdir \
    broadinstitute/gatk \
    samtools view -b -h -F 0x400 \
    -o /workdir/R016_WT_1.clara_parabrics.deduplicated.bam \
    /workdir/R016_WT_1.clara_parabrics.duplicates_marked.bam


# gatk MarkDuplicates \
#   -I input.bam \
#   -O output_dedup.bam \
#   -M output_metrics.txt \
#   --REMOVE_DUPLICATES true


###########################################################################################



### linux
docker run --gpus all --rm \
    --volume /data0/rawdata/SRR8117355:/workdir \
    --volume /data0/output/SRR8117355:/outputdir \
    --volume /mnt/genomes/human_genome:/genomes \
    --workdir /workdir \
    --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 \
    nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 pbrun fq2bam_meth \
    --ref /genomes/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
    --in-fq /workdir/SRR8117355_1.fastq.gz /workdir/SRR8117355_2.fastq.gz \
    --out-bam /outputdir/SRR8117355.clara_parabrics.duplicates_marked.bam \
    --out-qc-metrics-dir /outputdir/qc-metrics.SRR8117355 \
    --out-duplicate-metrics /outputdir/deduplicate_metrics.SRR8117355.txt \
    --logfile /outputdir/log_SRR8117355.fq2bam_meth.log \
    --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite 


### Windows Power Shell
docker run --gpus all --rm --volume D:/samples/breast_cancer/SRR8117355:/workdir --volume D:/samples/breast_cancer/output/SRR8117355:/outputdir --volume D:/genomes/human_genome:/genomes --workdir /workdir --env TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=268435456 nvcr.io/nvidia/clara/clara-parabricks:4.3.2-1 pbrun fq2bam_meth --ref /genomes/Homo_sapiens.GRCh38.dna.primary_assembly.fa --in-fq /workdir/SRR8117355_1.fastq.gz /workdir/SRR8117355_2.fastq.gz --out-bam /outputdir/SRR8117355.clara_parabrics.duplicates_marked.bam --out-qc-metrics-dir /outputdir/qc-metrics.SRR8117355 --out-duplicate-metrics /outputdir/deduplicate_metrics.SRR8117355.txt --logfile /outputdir/log_SRR8117355.fq2bam_meth.log --tmp-dir /workdir --bwa-cpu-thread-pool 16 --gpusort --gpuwrite 


########## Deduplicate

docker run --rm \
    --volume /data0/output/SRR8117355:/workdir \
    --volume /data0/output/SRR8117355:/outputdir \
    --workdir /workdir \
    staphb/samtools:0.21 \
    samtools view -b -h -F 0x400 \
    -o /workdir/SRR8117355.clara_parabrics.deduplicated.bam \
    /workdir/SRR8117355.clara_parabrics.duplicates_marked.bam
    
########### Extract methylation
docker run --rm \
    --volume /data0/output/SRR8117355:/workdir \
    --volume /data0/output/SRR8117355:/outputdir \
    --volume /mnt/genomes/human_genome:/genomes \
    --workdir /outputdir \
    sainim3/methyldackel:0.1 \
    MethylDackel extract --CHG --CHH --cytosine_report \
    /genomes/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
    /workdir/SRR8117355.clara_parabrics.duplicates_marked.bam \
    -o SRR8117355
