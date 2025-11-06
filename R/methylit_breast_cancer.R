getwd()
library(MethylIT)

setwd("/data3/sarah/breast_cancer/MethylIT/")
folder = "/data3/sarah/breast_cancer/healthy"
files = list.files(path = folder, pattern = "_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", full.names = TRUE)

files2 = list.files(path = folder, pattern = "_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", full.names = FALSE)
sample.id = sub("_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", "", files2)
sample.id
#healthy_samples <- c("SRR8117446","SRR8117447","SRR8117448","SRR8117449",
               # "SRR8117450","SRR8117451","SRR8117452","SRR8117453",
               # "SRR8117454","SRR8117455","SRR8117456","SRR8117457",
               # "SRR8117458","SRR8117459","SRR8117460","SRR8117461",
               # "SRR8117462","SRR8117463","SRR8117464","SRR8117465",
               # "SRR8117410","SRR8117411","SRR8117412","SRR8117413",
               # "SRR8117414","SRR8117415","SRR8117416","SRR8117417",
               # "SRR8117419","SRR8117421","SRR8117423","SRR8117425",
               # "SRR8117426","SRR8117428","SRR8117430","SRR8117431",
               # "SRR8117432","SRR8117433","SRR8117434","SRR8117435"
               # )
healthy_samples <- paste0("healthy_", sample.id)
#save(healthy_samples, file = "/data3/sarah/breast_cancer/MethylIT/file_names_healthy.RData")
healthy_samples
Normal <- readCounts2GRangesList(filenames = files,
                             sample.id = healthy_samples,
                             columns = c(seqnames = 1, start = 2, strand = 3,
                                         mC = 4, uC = 5),
                             remove = FALSE, pattern = "^1[^0123456789_]",
                             verbose = TRUE)
save(Normal, file = "/data3/sarah/breast_cancer/MethylIT/chr1/meth_healthy_chr1.RData",compress = "xz")

load("/data3/sarah/breast_cancer/MethylIT/chr1/meth_healthy_chr1.RData")
meth_level_healthy <- meth_levels(GR=Normal,
                                  columns=c(mC1=1,uC1=2,mC2=NULL,uC2=NULL),
                                  Bayesian = FALSE,
                                  init.pars = NULL,
                                  via.optim = TRUE,
                                  min.coverage = 3,
                                  tv = FALSE,
                                  bay.tv = FALSE,
                                  filter = TRUE,
                                  preserve.dt = FALSE,
                                  loss.fun = c("linear", "huber", "smooth", "cauchy", "arctg"),
                                  num.cores = 1,
                                  tasks = 0L,
                                  verbose = TRUE,)

save(meth_level_healthy, file = "/data3/sarah/breast_cancer/MethylIT/chr1/meth_levels_healthy_chr1_filter.RData",compress = "xz")

###-----------------------------------------------Early Cancer----------------------------------###

setwd("/data3/sarah/breast_cancer/MethylIT/")
folder = "/data3/sarah/breast_cancer/early_cancer"
files = list.files(path = folder, pattern = "_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", full.names = TRUE)

files2 = list.files(path = folder, pattern = "_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", full.names = FALSE)
sample.id = sub("_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", "", files2)
sample.id
early_cancer_samples <- paste0("early_", sample.id)
early_cancer_samples
Early_Cancer <- readCounts2GRangesList(filenames = files,
                                 sample.id = early_cancer_samples,
                                 columns = c(seqnames = 1, start = 2, strand = 3,
                                             mC = 4, uC = 5),
                                 remove = FALSE, pattern = "^5[^0123456789_]",
                                 verbose = TRUE)
save(Early_Cancer, file = "/data3/sarah/breast_cancer/MethylIT/chr1/meth_early_cancer_chr1.RData",compress = "xz")


load("/data3/sarah/breast_cancer/MethylIT/chr2/meth_early_cancer_chr2.RData")
meth_level_early_cancer <- meth_levels(GR=Early_Cancer,
                                  columns=c(mC1=1,uC1=2,mC2=NULL,uC2=NULL),
                                  Bayesian = FALSE,
                                  init.pars = NULL,
                                  via.optim = TRUE,
                                  min.coverage = 3,
                                  tv = FALSE,
                                  bay.tv = FALSE,
                                  filter = TRUE,
                                  preserve.dt = FALSE,
                                  loss.fun = c("linear", "huber", "smooth", "cauchy", "arctg"),
                                  num.cores = 1,
                                  tasks = 0L,
                                  verbose = TRUE,)

save(meth_level_early_cancer, file = "/data3/sarah/breast_cancer/MethylIT/chr1/meth_levels_early_cancer_chr1.RData")


###-----------------------------------------------Late Cancer----------------------------------###

setwd("/data3/sarah/breast_cancer/MethylIT/")
folder = "/data3/sarah/breast_cancer/late_cancer"
files = list.files(path = folder, pattern = "_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", full.names = TRUE)

files2 = list.files(path = folder, pattern = "_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", full.names = FALSE)
sample.id = sub("_val_1_bismark_bt2_pe.deduplicated.CX_report.txt", "", files2)
sample.id
late_cancer_samples <- paste0("late_", sample.id)
late_cancer_samples
Late_Cancer <- readCounts2GRangesList(filenames = files,
                                       sample.id = late_cancer_samples,
                                       columns = c(seqnames = 1, start = 2, strand = 3,
                                                   mC = 4, uC = 5),
                                       remove = FALSE, pattern = "^X[^0123456789_]",
                                       verbose = TRUE)
save(Late_Cancer, file = "/data3/sarah/breast_cancer/MethylIT/chr1/meth_late_cancer_chr1.RData",compress = "xz")




###-----------------------------------------------Reference----------------------------------###

load("/data3/sarah/breast_cancer/MethylIT/chr1/meth_healthy_chr1.RData")
#meth <- lapply(Normal, 
 #              function(x) {
#                 m <- mcols(x)[, 1]
 #                return(x[ m > 0]) 
  #             })

meth1 <- lapply(Normal, 
               function(x) {
                 m <- rowSums(as.matrix(mcols(x)[,1:2]))
                 return(x[ m > 0]) 
               })

ref = poolFromGRlist(meth1, stat = "mean", num.cores = 40L, verbose = FALSE)

save(ref, file = "/data3/sarah/breast_cancer/MethylIT/chr1/ref_chr1.RData",
     compress = "xz")


###-----------------------------------------------Estimate Divergence----------------------------------###


load("/data3/sarah/breast_cancer/MethylIT/chr1/ref_chr1.RData")
load("/data3/sarah/breast_cancer/MethylIT/chr1/meth_early_cancer_chr1.RData")

meth1 <- lapply(Early_Cancer, 
                function(x) {
                  m <- rowSums(as.matrix(mcols(x)[,1:2]))
                  return(x[ m >= 4]) 
                })

div = estimateDivergence(ref = ref,
                         indiv = meth1,
                         Bayesian = TRUE,
                         min.meth = c(0,1),
                         min.coverage = c(2,4),
                         min.sitecov = 4,
                         JD = TRUE,
                         jd.stat=TRUE,
                         num.cores = 20L,
                         tasks = 0,
                         high.coverage = 600,
                         percentile = 1,
                         verbose = TRUE)

save(div, file = "/data3/sarah/breast_cancer/MethylIT/chr1/div_chr1_early_cancer.RData", compress="xz")



load("/data1/autism22/data/td/chrY/ref_chrY.RData")
load("/data1/autism22/data/asd/chrY/meth_asd.RData")

div = estimateDivergence(ref = ref,
                         indiv = ASD,
                         Bayesian = TRUE,
                         min.meth = 4,
                         min.coverage = 8,
                         min.sitecov = 4,
                         JD = TRUE,
                         num.cores = 80L,
                         tasks = 0,
                         high.coverage = 600,
                         percentile = 1,
                         verbose = TRUE)

save(div, file = "/data1/autism22/data/asd/chrY/div_chrY.RData", compress="xz")


load('/data1/autism22/data/asd/chr3/div_chr3.RData')

critical.val <- do.call(rbind, lapply(div, function(x) {
  hd.95 = quantile(x$hdiv, 0.95)
  tv.95 = quantile(abs(x$bay.TV), 0.95)
  return(c(tv = tv.95, hd = hd.95))
}))

critical.val
max(critical.val[, 1])


d <- c("Weibull2P", "Weibull3P", "Gamma2P", "Gamma3P", "GGamma3P", "GGamma4P")
load('/data1/autism22/data/asd/chrY/div_chrY.RData')
  
gof_hd <- gofReport(
  HD = div,
  model = d,
  column = 9L,
  num.cores = 50L,
  alt_models = TRUE,
  r.cv = TRUE,
  output = "all",
  verbose = FALSE)
  
save(gof_hd, file = '/data1/autism22/data/asd/chrY/gof_hd_chrY.RData', compress = "xz")


load('/data1/autism22/data/asd/chrY/div_chrY.RData')

gof_jd <- gofReport(
  HD = div,
  model = d,
  column = 10L,
  num.cores = 50L,
  alt_models = TRUE,
  r.cv = TRUE,
  output = "all",
  verbose = FALSE)

save(gof_jd, file = '/data1/autism22/data/asd/chrY/gof_jd_chrY.RData', compress = "xz")


load('/data1/autism22/data/td/chrY/div_chrY.RData')

critical.val <- data.frame(do.call(rbind, lapply(div, function(x) {
  hd.95 = quantile(x$hdiv, 0.95)
  tv.95 = quantile(abs(x$bay.TV), 0.95)
  return(c(tv = tv.95, hd = hd.95))
})))

critical.val

save(critical.val, file = '/data1/autism22/data/td/chrY/critical_val_chrY.RData', compress = "xz")


load('/data1/autism22/data/td/chr3/div_chr3.RData')
load('/data1/autism22/data/td/chr3/gof_hd_chr3.RData')

p_dimps_td <- getPotentialDIMP(
  LR = div, dist.name = gof_hd$bestModel,
  nlms = gof_hd$nlms, div.col = 9, alpha = 0.05,
  tv.col = 7, tv.cut = 0.3)

load('/data1/autism22/data/asd/chr3/div_chr3.RData')
load('/data1/autism22/data/asd/chr3/gof_hd_chr3.RData')

p_dimps_asd <- getPotentialDIMP(
  LR = div, dist.name = gof_hd$bestModel,
  nlms = gof_hd$nlms, div.col = 9, alpha = 0.05,
  tv.col = 7, tv.cut = 0.3)

pdimp <- c(p_dimps_td, p_dimps_asd)
class(pdimp) <- "pDMP"


# save(pdimp, file = '/data1/autism22/data/asd/chrY/pdimp_chrY.RData', compress = "xz")


# load('/data1/autism22/data/asd/chr1/pdimp_chr1.RData')

cutpoint1 = estimateCutPoint(LR = pdimp,
                             control.names = td_samples,
                             treatment.names = asd_samples,
                             simple = FALSE,
                             classifier1 = "pca.qda",
                             classifier2 = "logistic",
                             column = c(hdiv = TRUE, bay.TV = TRUE,
                                        wprob = TRUE, pos = TRUE),
                             interactions = c("wprob:hdiv"),
                             n.pc = 4 , center = TRUE, scale = TRUE,
                             post.cut = 0.6,
                             div.col = 9, clas.perf = TRUE)
cutpoint1$cutpoint
cutpoint1$testSetPerformance

save(cutpoint1, file = '/data1/autism22/data/asd/chr1/cutpoint_chr1.RData', compress = "xz")


