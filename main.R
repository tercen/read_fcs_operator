suppressPackageStartupMessages({
  library(tercen)
  library(tercenApi)
  library(dplyr, warn.conflicts = FALSE)
  library(flowCore)
  library(base64enc)
  library(tidyr)
})

source("./utils.R")

ctx <- tercenCtx()

if (!any(ctx$cnames == "documentId")) {
  stop("Column factor documentId is required")
}

which.lines <- ctx$op.value("which.lines", as.double, -1)
if(which.lines == -1) which.lines <- NULL

# 1. Extract files
files <- download_files(ctx)
files_prep <- prepare_files(files)
on.exit(unlink(files_prep$f.names))

assign("actual", 0, envir = .GlobalEnv)

# 2. Read and convert FCS files
df <- files_prep %>%
  apply(MARGIN = 1, function(fn) {
    
    out <- list()
    data <- get_fcs(fn["f.names"], which.lines)
    out$spill.matrix <- get_spill_matrix(data, separator = separator, ctx)
    out$spill.matrix$filename <- basename(fn["f.names"])
    
    tmp <- process_fcs(data)
    out$data <- tmp$fcs.data
    out$map <- tmp$names_map

    if(inherits(out$spill.matrix, "matrix")) {
      out$spill.matrix <- out$spill.matrix %>% 
        as_tibble() %>%
        ctx$addNamespace() %>%
        as.matrix()
    }
    
    actual = get("actual",  envir = .GlobalEnv) + 1
    ctx$progress(
      message = paste0('Processing FCS file: ' , fn["f.names"], '\n'),
      actual = actual,
      total = length(files_prep$f.names)
    )
    assign("actual", actual, envir = .GlobalEnv)

    out
  })

df_out <- lapply(df, "[[", "data") %>%
  bind_rows()

spill.list <- lapply(df, "[[", "spill.matrix") %>%
  bind_rows()

if(any(is.na(unlist(spill.list)))) {
  ctx$log(message = "No built-in compensation matrices found.")
} else {
  upload_df(
    spill.list,
    ctx,
    folder_name = "Compensation",
    prefix = "Compensation-",
    suffix = files$docname
  )
}

names.map <- lapply(df, "[[", "map") %>%
  bind_rows()

upload_df(
  names.map,
  ctx,
  folder_name = "Annotation",
  prefix = "Channel-Names-",
  suffix = files$docname
)

df_out %>% 
  ctx$addNamespace() %>%
  ctx$save()
