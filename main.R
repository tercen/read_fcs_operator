library(BiocManager)
library(tercen)
library(dplyr)
library(flowCore)

# http://localhost:53322/index.html#/alex/w/e11cea9d1119c8e6dbb0842925032c6e/ds/91c7e052-54e6-49b8-bf2c-09f55c5762e3
# options("tercen.serviceUri"="http://172.17.0.1:5400/api/v1/")
# options("tercen.workflowId"= "e11cea9d1119c8e6dbb0842925032c6e")
# options("tercen.stepId"= "91c7e052-54e6-49b8-bf2c-09f55c5762e3")
 
doc_to_data = function(df){
  filename = tempfile()
  writeBin(ctx$client$fileService$download(df$documentId[1]), filename)
  on.exit(unlink(filename))
  data_fcs = read.FCS(filename, transformation = FALSE)
  names_parameters = data_fcs@parameters@data$desc
  data = as.data.frame(exprs(data_fcs))
  col_names = colnames(data)
  names_parameters = ifelse(is.na(names_parameters),col_names,names_parameters)
  colnames(data) = names_parameters
  print(class(data))
  data %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci= rep_len(df$.ci[1], nrow(.)))
}

ctx = tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 

ctx$cselect() %>% 
  mutate(.ci= 1:nrow(.)-1) %>%
  split(.$.ci) %>%
  lapply(doc_to_data) %>%
  bind_rows() %>%
  ctx$addNamespace() %>%
  ctx$save()



