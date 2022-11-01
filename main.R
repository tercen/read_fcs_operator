suppressPackageStartupMessages({
  library(tercen)
  library(tercenApi)
  library(dplyr, warn.conflicts = FALSE)
  library(flowCore)
  library(base64enc)
})

source("./utils.R")

ctx <- tercenCtx()

if (!any(ctx$cnames == "documentId")) {
  stop("Column factor documentId is required")
}

use.descriptions <- ctx$op.value("use.descriptions", as.logical, TRUE)
separator <- ctx$op.value("Separator", as.character, "Comma")
which.lines <- ctx$op.value("which.lines", as.double, -1)
if(which.lines == -1) which.lines <- NULL

# 1. Extract files
files <- download_files(ctx)
files_prep <- prepare_files(files)
on.exit(unlink(files_prep$f.names))

assign("actual", 0, envir = .GlobalEnv)
task = ctx$task

# 2. Read and convert FCS files
df <- files_prep %>%
  apply(MARGIN = 1, function(fn) {
    
    out <- list()
    data <- get_fcs(fn["f.names"], which.lines)
    out$spill.matrix <- get_spill_matrix(data, csv.comp = fn["c.names"], separator = separator)
    if(inherits(out$spill.matrix, "matrix")) {
      out$spill.matrix <- out$spill.matrix %>% 
        as_tibble() %>%
        ctx$addNamespace() %>%
        as.matrix()
    }

    out$data <- process_fcs(data, use.descriptions)

    if (!is.null(task)) {
      # task is null when run from RStudio
      actual = get("actual",  envir = .GlobalEnv) + 1
      assign("actual", actual, envir = .GlobalEnv)
      evt = TaskProgressEvent$new()
      evt$taskId = task$id
      evt$total = length(files_prep$f.names)
      evt$actual = actual
      evt$message = paste0('Processing FCS file: ' , fn["f.names"], '\n')
      ctx$client$eventService$sendChannel(task$channelId, evt)
    } else {
      cat('Processing FCS file: ' , fn["f.names"], '\n')
    }
    out
  })

df_out <- lapply(df, "[[", "data") %>%
  bind_rows()

spill.list <- lapply(df, "[[", "spill.matrix")

if(any(is.na(unlist(spill.list)))) {

  ctx$log(message = "No built-in compensation matrices found.")
  df_out %>% 
    ctx$addNamespace() %>%
    ctx$save()
  
} else {

  names(spill.list) <- basename(files_prep$f.names)
  
  df_comp <- data.frame(
    compensation_matrices = "compensation_matrices",
    .base64.serialized.r.model = c(serialize_to_string(spill.list))
  ) %>% 
    ctx$addNamespace() %>%
    as_relation()
  
  df_out %>%
    ctx$addNamespace() %>%
    as_relation() %>%
    left_join_relation(ctx$crelation, ".ci", ctx$crelation$rids) %>%
    left_join_relation(df_comp, list(), list()) %>%
    as_join_operator(ctx$cnames, ctx$cnames) %>%
    save_relation(ctx)

}
