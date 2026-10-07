# flowCore reference with the exact read_fcs_operator settings (utils.R get_fcs).
suppressMessages(library(flowCore)); args <- commandArgs(TRUE); f <- args[1]; out <- args[2]
ff <- suppressWarnings(read.FCS(f, transformation = FALSE, which.lines = NULL, dataset = 2, emptyValue = FALSE, ignore.text.offset = TRUE, truncate_max_range = TRUE))
m <- exprs(ff); colnames(m) <- gsub(",", "_", colnames(m))
write.csv(as.data.frame(m), out, row.names = FALSE)
cat(sprintf("%s: %d x %d\n", basename(f), nrow(m), ncol(m)))
