library(BiocManager)
library(tercen)
library(dplyr)
library(flowCore)


fcs_to_data = function(filename, which.lines, alter.names) {
  data_fcs = read.FCS(filename, 
                      which.lines = which.lines,
                      transformation = FALSE, 
                      alter.names = alter.names)
  names_parameters = data_fcs@parameters@data$desc
  data = as.data.frame(exprs(data_fcs))
  col_names = colnames(data)
  names_parameters = ifelse(is.na(names_parameters),col_names,names_parameters)
  colnames(data) = names_parameters
  data %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci = as.integer(rep_len(0, nrow(.)))) %>%
    mutate(filename = rep_len(basename(filename), nrow(.)))
}

ctx = tercenCtx()
 
if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required")

which.lines <- NULL
if(!is.null(ctx$op.value('which.lines')) && !ctx$op.value('which.lines') == "NULL") which.lines <- as.integer(ctx$op.value('which.lines'))

# extract files
df <- ctx$cselect()

docId = df$documentId[1]
doc = ctx$client$fileService$get(docId)
filename = tempfile()
writeBin(ctx$client$fileService$download(docId), filename)
on.exit(unlink(filename))

# unzip if archive
if(length(grep(".zip", doc$name)) > 0) {
  tmpdir <- tempfile()
  unzip(filename, exdir = tmpdir)
  f.names <- list.files(tmpdir, full.names = TRUE)
} else {
  f.names <- filename
}

# check FCS
if(any(!isFCSfile(f.names))) stop("Not all imported files are FCS files.")

assign("actual", 0, envir = .GlobalEnv)
task = ctx$task


# convert them to FCS files
f.names %>%
  lapply(function(filename){
    data = fcs_to_data(filename, which.lines, as.logical(ctx$op.value('alter.names')))
    if (!is.null(task)) {
      # task is null when run from RStudio
      actual = get("actual",  envir = .GlobalEnv) + 1
      assign("actual", actual, envir = .GlobalEnv)
      evt = TaskProgressEvent$new()
      evt$taskId = task$id
      evt$total = length(f.names)
      evt$actual = actual
      evt$message = paste0('processing FCS file ' , filename)
      ctx$client$eventService$sendChannel(task$channelId, evt)
    } else {
      cat('processing FCS file ' , filename)
    }
    data
  }) %>%
  bind_rows() %>%
  ctx$addNamespace() %>%
  ctx$save()
