suppressPackageStartupMessages({
  library(tercen)
  library(dplyr, warn.conflicts = FALSE)
  library(flowCore)
  library(base64enc)
  library(tidyr)
  library(data.table)
})

source("./utils.R")

ctx <- tercenCtx()

if (!any(ctx$cnames == "documentId")) {
  stop("Column factor documentId is required")
}

which.lines <- ctx$op.value("which.lines", as.double, -1)
if(which.lines == -1 | is.na(which.lines)) which.lines <- NULL
do.gather <- ctx$op.value("gather", as.logical, TRUE)
ungather_pattern <- ctx$op.value("ungather_pattern", as.character, "time|event")

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
    
    tmp <- process_fcs(data, do.gather = do.gather, ungather_pattern)
    out$data <- tmp$fcs.data
    out$map <- tmp$names_map
    out$fcs_name <- tmp$fcs_name
    
    if(inherits(out$spill.matrix, "matrix")) {
      out$spill.matrix <- out$spill.matrix %>% 
        as_tibble() %>%
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

fcs_nm <- unlist(lapply(df, "[[", "fcs_name"))
fcs <- seq_along(fcs_nm)
names(fcs) <- fcs_nm

df_out <- lapply(df, "[[", "data") 
df_out <- mapply(cbind, df_out, "fileId" = fcs, SIMPLIFY = F) %>%
  bind_rows() %>%
  as.data.table() %>%
  mutate(event_id = seq_len(nrow(.)))

event_table <- df_out %>%
  select(matches(ifelse(do.gather, "[a-zA-Z]", "fileId|event_id"))) %>% 
  distinct() %>%
  mutate(filename = names(fcs)[fileId]) %>%
  select(-fileId) %>%
  as_relation()

if(do.gather) {
  expression_table <- df_out %>% 
    select(matches("[0-9]+|event_id")) %>%
    melt(
      id.vars = c("event_id"),
      value.name = "value", variable.name = "channel_id"
    ) %>%
    mutate(channel_id = as.integer(channel_id)) %>%
  arrange(event_id, channel_id) %>% 
  as_tibble()
} else {
  expression_table <- df_out
}

spill.list <- lapply(df, "[[", "spill.matrix") %>%
  bind_rows()

names.map <- lapply(df, "[[", "map") %>%
  bind_rows() %>%
  as.data.table() %>%
  select(channel_name, channel_description, channel_id) %>%
  distinct()

bad_description <- names.map %>% 
  select(channel_name) %>% 
  duplicated() %>%
  any()

if(bad_description) {
  ctx$log(message = "Different descriptions for the same channel name have been found. Description field will be ignored.")
  marker_table <- names.map %>% 
    select(channel_name, channel_id) %>%
    distinct() %>% 
    as_relation()
} else {
  marker_table <- names.map %>% as_relation()
}

rel_out <- expression_table %>% 
  as_relation %>%
  left_join_relation(event_table, "event_id", "event_id")

if(do.gather) {
  rel_out <- rel_out %>% left_join_relation(marker_table, "channel_id", "channel_id")
}

if(any(is.na(unlist(spill.list)))) {
  ctx$log(message = "No built-in compensation matrices found.")
  rel_out %>%
    as_join_operator(list(), list()) %>%
    save_relation(ctx)
} else {
  spill.list <- spill.list %>% as_relation %>% as_join_operator(list(), list())
  rel_out <- rel_out %>% as_join_operator(list(), list()) 
  save_relation(list(rel_out, spill.list), ctx)
}
