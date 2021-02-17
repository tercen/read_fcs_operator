library(BiocManager)
library(tercen)
library(dplyr)
library(flowCore)
# library(Biobase)

# getOption("tercen.serviceUri")
# getOption("tercen.username")
# getOption("tercen.password")
# 
# options("tercen.username"="alex")
# options("tercen.password"="aa")
 
# filename = '~/projects/test.fcs'
# data_fcs = read.FCS(filename, transformation = FALSE)
# http://127.0.0.1:5400/alex/w/8651421aa0d724a578ae9faa1e01c74d/ds/ebda5081-aae4-45fc-b91b-f7b4b19d402a
# options("tercen.serviceUri"="http://172.17.0.1:5400/api/v1/")
# options("tercen.workflowId"= "8651421aa0d724a578ae9faa1e01c74d")
# options("tercen.stepId"= "ebda5081-aae4-45fc-b91b-f7b4b19d402a")

# ctx = tercenCtx()
# docId = '1442fa719e5ff43509a9b5ac43001848'
# doc = ctx$client$fileService$get(docId)
# filename = doc$name
# data = ctx$client$fileService$download(docId)
# writeBin(data, filename)
# writeBin(ctx$client$fileService$download(docId), filename)
# 
# fds_to_data(filename)
 
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
