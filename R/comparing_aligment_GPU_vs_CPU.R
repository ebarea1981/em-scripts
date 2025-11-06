library(MethylIT)
library(BiocParallel)

## ===================================================================== ##
## ============================  Data initialization ====================
## ===================================================================== ##
WORKING_DIR <- "/home/ebarea/0_trabajo/0_tests_remove_later/methylation_report_data"
ROWDATA_DIR <- "/home/ebarea/0_trabajo/0_tests_remove_later/methylation_report_data"

setwd(WORKING_DIR)
getwd()

### CHROMOSOME NAME
CHR_NAME <- 'chr1'
CHR_NAME_PATTERN <- '1'

### Some variables
CONTEXT_LIST <- c("CG", "CHG", "CHH")
num.cores <- 10

DATA_DIR <- paste0(WORKING_DIR, '/data/', CHR_NAME)
if (!dir.exists(DATA_DIR)) dir.create(path = DATA_DIR, recursive = TRUE)

### RESULT VAR names
readcounts_result_var <- paste0('readcounts.', CHR_NAME)
ref_result_var <- paste0('ref.', CHR_NAME)
y.centroid_result_var <- paste0('y.centroid.', CHR_NAME)
hdiv_result_var <- paste0('hdiv.', CHR_NAME)
ent_result_var <- paste0('ent.', CHR_NAME)

### UTIL FUNCTIONS
getRDataFullPath <- function(rdata_name) {
     rdata.path <- paste0(DATA_DIR, '/', rdata_name, '.RData')
     return(rdata.path)
}

load_rdata_into_object <- function(f)
{
     env <- new.env()
     nm <- load(f, env)[1]
     env[[nm]]
}

## ===================================================================== ##
## ============================  Readcounts TEST PARALLEL ===============
## ===================================================================== ##

filenames.gpu <- c(
     'COL_R12_2.cytosine_report.txt',
     'cas_R12_1.cytosine_report.txt', 
     'cas_R12_3.cytosine_report.txt'
)
sample_id.gpu <- c(
     'GPU_Co1', 'GPU_cas1', 'GPU_cas3'
)


filenames.cpu <- c( # Treatment
     'COL_R12_2_val_1_bismark_bt2_pe.deduplicated.CX_report.txt', 
     'cas_R12_1_val_1_bismark_bt2_pe.deduplicated.CX_report.txt',
     'cas_R12_3_val_1_bismark_bt2_pe.deduplicated.CX_report.txt'
)
sample_id.cpu <- c(
     'CPU_Co1', 'CPU_cas1', 'CPU_cas3'
)


columns <- c(seqnames = 1, start = 2, strand = 3, mC = 4, uC = 5, context = 6, signal = 7)
pattern <- paste0('^', CHR_NAME_PATTERN, '[^0123456789_]')

###- GPU samples ====
filenames_fullpath <- paste0(ROWDATA_DIR, '/', filenames.gpu)

t1 <- system.time(
     readcounts.gpu <- readCounts2GRangesList(
          filenames = filenames_fullpath,
          sample.id = sample_id.gpu,
          columns = columns,
          pattern = pattern
          ,nThread = num.cores
     )
)
cat(t1[3]/60, "minutes.", date()) 

readcounts.gpu <- filterByCoverage(
     readcounts.gpu,
     min.coverage = 1,
     percentile = 1,
     col.names = c(mC=1, uC=2 ),
     num.cores = num.cores
)

for (context in CONTEXT_LIST) {
     lr <- lapply(readcounts.gpu, function(item) {
          item <- item[ item$context == context]
          return(item)
     })
     readcounts.path <- getRDataFullPath(
          paste0(readcounts_result_var, ".gpu.", context)
     )
     save(lr, file = readcounts.path)
     message('*** SAVED FILE ===> ', readcounts.path)
     rm(lr, readcounts.path); gc()
}

readcounts.path <- getRDataFullPath(paste0(readcounts_result_var, ".gpu"))
save(readcounts.gpu, file = readcounts.path)
cat('*** SAVED FILE ===> ', readcounts.path)


###- CPU samples ====
filenames_fullpath <- paste0(ROWDATA_DIR, '/', filenames.cpu)

t1 <- system.time(
     readcounts.cpu <- readCounts2GRangesList(
          filenames = filenames_fullpath,
          sample.id = sample_id.cpu,
          columns = columns,
          pattern = pattern
          ,nThread = num.cores
     )
)
cat(t1[3]/60, "minutes.", date()) 

readcounts.cpu <- filterByCoverage(
     readcounts.cpu,
     min.coverage = 1,
     percentile = 1,
     col.names = c(mC=1, uC=2 ),
     num.cores = num.cores
)

for (context in CONTEXT_LIST) {
     message("\n*** Processing context ", context, " ...\n")
     
     lr <- lapply(readcounts.cpu, function(item) {
          item <- item[ item$context == context]
          return(item)
     })
     readcounts.path <- getRDataFullPath(
          paste0(readcounts_result_var, ".cpu.", context)
     )
     save(lr, file = readcounts.path)
     message('*** SAVED FILE ===> ', readcounts.path)
     rm(lr, readcounts.path); gc()
}

readcounts.path <- getRDataFullPath(paste0(readcounts_result_var, ".cpu"))
save(readcounts.cpu, file = readcounts.path)
cat('*** SAVED FILE ===> ', readcounts.path)


readcounts <- c(readcounts.gpu, readcounts.cpu)
readcounts.path <- getRDataFullPath(paste0(readcounts_result_var))
save(readcounts, file = readcounts.path)


#### ------------------------  Coverage Report --------------------

## 'histogram_coverage_report.Rmd' address in the system
rfile <- system.file("histogram_coverage_report_by_context.Rmd", package="MethylIT",
                     lib.loc = .libPaths()[1])
#-- CG

file.path <- getRDataFullPath(
     paste0(readcounts_result_var, ".gpu.", "CG")
)
message('*** Loading samples from "', file.path, '" ...\n')
lr_cg <- load_rdata_into_object(file.path)

file.path <- getRDataFullPath(
     paste0(readcounts_result_var, ".cpu.", "CG")
)
message('*** Loading samples from "', file.path, '" ...\n')
lr_cg <- c(lr_cg, load_rdata_into_object(file.path))

#-- CHG
file.path <- getRDataFullPath(
     paste0(readcounts_result_var, ".gpu.", "CHG")
)
message('*** Loading samples from "', file.path, '" ...\n')
lr_chg <- load_rdata_into_object(file.path)

file.path <- getRDataFullPath(
     paste0(readcounts_result_var, ".cpu.", "CHG")
)
message('*** Loading samples from "', file.path, '" ...\n')
lr_chg <- c(lr_chg, load_rdata_into_object(file.path))

#-- CHH
file.path <- getRDataFullPath(
     paste0(readcounts_result_var, ".gpu.", "CHH")
)
message('*** Loading samples from "', file.path, '" ...\n')
lr_chh <- load_rdata_into_object(file.path)

file.path <- getRDataFullPath(
     paste0(readcounts_result_var, ".cpu.", "CHH")
)
message('*** Loading samples from "', file.path, '" ...\n')
lr_chh <- c(lr_chh, load_rdata_into_object(file.path))

#-
dir <- paste0(DATA_DIR,'/graphics/')
if (!dir.exists(dir))
     dir.create(dir)

ids <- names(lr_cg)
read_stat <- vector(mode = "list", length = length(ids))

to <- paste0(dir, "histogram_coverage_report_by_context.Rmd")
file.copy(rfile, to, overwrite = TRUE)

for (j in 1:length(ids)) {
     short_id = ids[j]
     filename <- paste0("dbtyp2_", short_id)
     
     message("...\n---- Processing sample: ", short_id, "...\n")
     
     idx_data <- match(short_id, ids)
     
     ## --------- CG context ---------#
     file <- paste0(dir, filename,"_cov_", CHR_NAME, "_cg")
     cg <- unlist(lr_cg[idx_data])
     sum_rep_cg <- hist_report(cg, 
                               columns = 1:2,
                               main = paste0("Histogram of CG. ", short_id),
                               kden = TRUE,
                               xlim = c(0, 50),
                               min.val = 4,
                               bw = NULL,
                               annt = TRUE,
                               summary = TRUE,
                               sub = NA,
                               #xlab = paste0("Coverage - ", CHR_NAME),
                               filename = file,
                               cex.main = 4,
                               cex.sub = 3,
                               cex.lab = 3,
                               cex.axis = 3,
                               cex.text = 3,
                               xaxis.label.line = 4,
                               mar = c(6, 6, 8, 2),
                               col.main = "blue",
                               col.sub = "red")
     
     ## --------- CHG context ---------#
     file <- paste0(dir, filename,"_cov_", CHR_NAME, "_chg")
     #chg <- lr[lr$context == "CHG"]
     chg <- unlist(lr_chg[idx_data])
     
     sum_rep_chg <- hist_report(chg, 
                                columns = 1:2,
                                main = paste0("Histogram of CHG. ", short_id),
                                kden = TRUE,
                                xlim = c(0, 50),
                                min.val = 4,
                                bw = NULL,
                                annt = TRUE,
                                summary = TRUE,
                                sub = NA,
                                xlab = paste0("Coverage - ", CHR_NAME),
                                filename = file,
                                cex.main = 4,
                                cex.sub = 3,
                                cex.lab = 3,
                                cex.axis = 3,
                                cex.text = 3,
                                xaxis.label.line = 4,
                                mar = c(6, 6, 8, 2),
                                col.main = "blue",
                                col.sub = "red")
     
     ## --------- CHH context ---------#
     file <- paste0(dir, filename,"_cov_", CHR_NAME, "_chh")
     #chh <- lr[lr$context == "CHH"]
     chh <- unlist(lr_chh[idx_data])
     
     sum_rep_chh <- hist_report(chh, 
                                columns = 1:2,
                                main = paste0("Histogram of CHH. ", short_id),
                                kden = TRUE,
                                xlim = c(0, 50),
                                min.val = 4,
                                bw = NULL,
                                annt = TRUE,
                                summary = TRUE,
                                sub = NA,
                                xlab = paste0("Coverage - ", CHR_NAME),
                                filename = file,
                                cex.main = 4,
                                cex.sub = 3,
                                cex.lab = 3,
                                cex.axis = 3,
                                cex.text = 3,
                                xaxis.label.line = 4,
                                mar = c(6, 6, 8, 2),
                                col.main = "blue",
                                col.sub = "red")
     
     read_stat[[j]] <- rbind(cg = sum_rep_cg, chg = sum_rep_chg,
                             chh = sum_rep_chh)
}
names(read_stat) <- ids

filename <- paste0(dir, "dbtyp2_cov_summary", CHR_NAME, "_cg-chg-chh")
save(read_stat, file = paste0(filename, ".RData"))

rmarkdown::render(
     input = to, 
     output_format = "html_document",
     output_file = paste0(dir, "histogram_coverage_report_", 
                          CHR_NAME, ".html"))

file.remove(to)

files <- list.files(path = dir, pattern = ".png")
file.remove(paste0(dir, files))


## ===================================================================== ##
## ============================  Step 2. Reference Sample ==============
## ===================================================================== ##

#context = 'CG'
for (context in CONTEXT_LIST) {
     message("\n*** Processing context ", context, " ...\n")
     
     # GPU
     readcounts.path <- getRDataFullPath(
          paste0(readcounts_result_var, ".gpu.", context)
     )
     message('*** Loading gpu samples from "', readcounts.path, '" ...\n')
     lr <- load_rdata_into_object(readcounts.path)
     
     t1 <- system.time(
          ref <- poolFromGRlist(
               lr,
               stat = "mean", 
               num.cores = num.cores,
               extr.column = "signal",
               verbose = TRUE)
     )
     message(t1[3]/60, " minutes.", date(), " ...\n")
     
     ref.path <- getRDataFullPath(paste0(ref_result_var, ".gpu.", context))
     save(ref, file = ref.path)
     
     message('*** SAVED FILE ===> ', ref.path, " ...\n")
     
     # CPU
     
     readcounts.path <- getRDataFullPath(
          paste0(readcounts_result_var, ".cpu.", context)
     )
     message('*** Loading gpu samples from "', readcounts.path, '" ...\n')
     lr <- load_rdata_into_object(readcounts.path)
     
     t1 <- system.time(
          ref <- poolFromGRlist(
               lr,
               stat = "mean", 
               num.cores = num.cores,
               extr.column = "signal",
               verbose = TRUE)
     )
     message(t1[3]/60, " minutes.", date(), " ...\n")
     
     ref.path <- getRDataFullPath(paste0(ref_result_var, ".cpu.", context))
     save(ref, file = ref.path)
     
     message('*** SAVED FILE ===> ', ref.path, " ...\n")
}

## ===================================================================== ##
## ============================  Step 4. Estimate divergence ===========
## ===================================================================== ##

t2 <- system.time(
     hdiv.GPU <- for (context in CONTEXT_LIST) {
          message("\n*** Processing context ", context, " ...\n")
          
          # GPU
          
          readcounts.path <- getRDataFullPath(
               paste0(readcounts_result_var, ".gpu.", context)
          )
          message('*** Loading samples from "', readcounts.path, '" ...\n')
          indiv <- load_rdata_into_object(readcounts.path)
          
          ref.path <- getRDataFullPath(paste0(ref_result_var, ".gpu.", context))
          message("*** Loading reference sample from ", ref.path, " ...\n")
          ref <- load_rdata_into_object(ref.path)
          
          t1 <- system.time(
               hdiv <- estimateDivergence(
                    ref = ref,
                    indiv = indiv,
                    Bayesian = TRUE,
                    min.meth = 1,
                    min.coverage = c(4, 4),
                    and.min.cov = TRUE,
                    min.sitecov = 4,
                    JD = TRUE,
                    num.cores = num.cores,
                    tasks = 0,
                    high.coverage = NULL,
                    percentile = 0.99999,
                    verbose = TRUE)
          )
          message("*** estimateDivergence last ", t1[3]/60, 
                  " minutes for context ",context, ".", date(), " ...\n")
          
          hdiv.path <- getRDataFullPath(paste0(hdiv_result_var, ".gpu.", context))
          
          save(hdiv, file = hdiv.path)
          message('*** SAVED FILE ===> ', hdiv.path)
          
          rm(readcounts.path, indiv, hdiv.path); gc()
     }
)
message("*** estimateDivergence for all context last ", t2[3]/60, 
        " minutes. ", date(), " ...\n")


t2 <- system.time(
     for (context in CONTEXT_LIST) {
          message("\n*** Processing context ", context, " ...\n")
          
          # CPU
          
          readcounts.path <- getRDataFullPath(
               paste0(readcounts_result_var, ".cpu.", context)
          )
          message('*** Loading samples from "', readcounts.path, '" ...\n')
          indiv <- load_rdata_into_object(readcounts.path)
          
          ref.path <- getRDataFullPath(paste0(ref_result_var, ".cpu.", context))
          message("*** Loading reference sample from ", ref.path, " ...\n")
          ref <- load_rdata_into_object(ref.path)
          
          t1 <- system.time(
               hdiv <- estimateDivergence(
                    ref = ref,
                    indiv = indiv,
                    Bayesian = TRUE,
                    min.meth = 1,
                    min.coverage = c(4, 4),
                    and.min.cov = TRUE,
                    min.sitecov = 4,
                    JD = TRUE,
                    num.cores = num.cores,
                    tasks = 0,
                    high.coverage = NULL,
                    percentile = 0.99999,
                    verbose = TRUE)
          )
          message("*** estimateDivergence last ", t1[3]/60, 
                  " minutes for context ",context, ".", date(), " ...\n")
          
          hdiv.path <- getRDataFullPath(paste0(hdiv_result_var, ".cpu.", context))
          
          save(hdiv, file = hdiv.path)
          message('*** SAVED FILE ===> ', hdiv.path)
          
          rm(readcounts.path, indiv, hdiv.path); gc()
     }
)
message("*** estimateDivergence for all context last ", t2[3]/60, 
        " minutes. ", date(), " ...\n")


## ===================================================================== ##
## ============================  Numerical Entropy =====================
## ===================================================================== ##

library(reshape2)
library(dplyr)

## -------------- Setting parallel computation --------------- #
bpparam <- MulticoreParam(workers = num.cores, tasks = 0,
                          progressbar = TRUE)
## ----------------------------------------------------------- #

context <- 'CG' 
t2 <- system.time(
     ent <- bplapply(CONTEXT_LIST, function(context) {
          message("\n*** Processing context ", context, " ...\n")
          
          hdiv.path <- getRDataFullPath(paste0(hdiv_result_var, ".gpu.", context))
          hdiv <- load_rdata_into_object(hdiv.path)
          cl <- class(hdiv)
          
          hdiv.path <- getRDataFullPath(paste0(hdiv_result_var, ".cpu.", context))
          hdiv <- c(hdiv, load_rdata_into_object(hdiv.path))
          
          class(hdiv) <- cl
          ent <- entropy(hdiv)
          ent <- unlist(ent)
          
          ent.path <- getRDataFullPath(paste0(ent_result_var, ".", context))
          
          #save(ent, file = ent.path)
          message('*** SAVED FILE ===> ', ent.path)
          
          rm(hdiv.path, hdiv, ent.path); gc()
          
          return(ent)
     },
     BPPARAM = bpparam
     )
)
message("*** Numerical Entropy computing for all context last ", t2[3]/60, 
        " minutes. ", date(), " ...\n")
names(ent) <- CONTEXT_LIST
ent


entropies <- matrix(data = NA, ncol = length(ent[[1]]), nrow = length(CONTEXT_LIST))
colnames(entropies) <- names(ent[[1]])
rownames(entropies) <- names(ent)

for (i in 1:length(CONTEXT_LIST)) {
     for(j in 1:length(ent[[1]])) {
          entropies[i, j] = ent[[i]][[j]]
     }
}


ent_data <- melt(entropies)
colnames(ent_data) <- c("context", "status", "entropy")

group <- rep("CT", nrow(ent_data))
group[ grep("T", ent_data$status) ] <- "DMT2"

ent_data$group <- factor(group, levels = c("CT", "DMT2"))
ent_data$context <- factor(ent_data$context, levels = CONTEXT_LIST)


box_plot(formula = entropy ~ group:context, 
         main = paste0("CG Methylation chrom", CHR_NAME),
         xlab = "Context",
         ylab = "Entropy",
         data = ent_data, 
         ylim = c(-2.5, 0),
         boxlwd = 0.1,
         boxwex = 0.7,
         tcl = -0.3,
         boxfill = c("dodgerblue", "salmon"),
         xticks.val.adj = c(0.9, 2.05),
         xaxis.label.line = 4,
         yticks.val.adj = c(0.6, 0.5),
         xtick.val.rot = 90,
         mar = c(12,4,2,2))
legend(1, -1.2, c("CT", "DM-Type-2"), bty = "n",
       fill = c("dodgerblue", "salmon"), 
       border = c("dodgerblue", "salmon"))


