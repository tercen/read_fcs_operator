library(BiocManager)
library(tercen)
library(dplyr)
library(flowCore)
# library(Biobase)


# filename = '~/projects/test.fcs'
# data_fcs = read.FCS(filename, transformation = FALSE)

# options("tercen.workflowId"= "132ff39e2530caca083900267b00283f")
# options("tercen.stepId"= "124f89fc-f3b3-4ebd-9abc-3f26d3f0fe61")

 
fds_to_data = function(filename) {
  
  data_fcs = read.FCS(filename, transformation = FALSE)
  names_parameters = data_fcs@parameters@data$desc
  data = as.data.frame(exprs(data_fcs))
  col_names = colnames(data)
  names_parameters = ifelse(is.na(names_parameters),col_names,names_parameters)
  colnames(data) = names_parameters
  data %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci = rep_len(0, nrow(.))) %>%
    mutate(filename = rep_len(basename(filename), nrow(.)))
}

ctx = tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 

#1. extract files
df <- ctx$cselect()

docId = df$documentId[1]
doc = ctx$client$fileService$get(docId)
filename = doc$name
writeBin(ctx$client$fileService$download(docId), filename)
on.exit(unlink(filename))

# unzip if archive
if(length(grep(".zip", filename)) > 0) {
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

assign

#2. convert them to FDS files
df <- f.names %>%
  lapply(function(filename){
    data = fds_to_data(filename)
    if (!is.null(task)) {
      # task is null when run from RStudio
      actual = get("actual",  envir = .GlobalEnv) + 1
      assign("actual", actual, envir = .GlobalEnv)
      evt = TaskProgressEvent$new()
      evt$taskId = task$id
      evt$total = length(f.names)
      evt$actual = actual
      evt$message = paste0('processing FDS file ' , filename)
      ctx$client$eventService$sendChannel(task$channelId, evt)
    } else {
      cat('processing FDS file ' , filename)
    }
    data
  }) %>%
  bind_rows() %>%
  ctx$addNamespace() %>%
  ctx$save()
