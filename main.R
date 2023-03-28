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
  bind_rows() %>%
  mutate(eventId = as.integer(seq_len(nrow(.)))) # %>%
  # ctx$addNamespace()

expression_table <- pivot_longer(
  df_out %>% select(-filename),
  cols = !contains(c("filename", ".ci", "eventId")),
  names_to = "channel",
  values_to = "value",
  names_transform = list(channel = as.factor)
)

event_table <- df_out %>%
  as_tibble() %>%
  select(filename, eventId) %>% 
  distinct()

marker_table <- tibble(
  channel_name = levels(expression_table$channel)
) %>%
  mutate(channel_id = seq_len(nrow(.)))

expression_table <- expression_table %>%
  mutate(channel_id = as.integer(channel))

spill.list <- lapply(df, "[[", "spill.matrix") %>%
  bind_rows()

names.map <- lapply(df, "[[", "map") %>%
  bind_rows()

names.map <- names.map %>% 
  select(name, description) %>% 
  distinct() %>% 
  rename(channel_name = name)

bad_description <- names.map %>% 
  select(channel_name) %>% 
  duplicated() %>%
  any()

if(bad_description) {
  ctx$log(message = "Different descriptions for the same channel name have been found. Description field will be ignored.")
} else {
  marker_table <- marker_table %>% left_join(names.map, by = "channel_name")
}

rel_out <- expression_table %>% 
  as_relation %>%
  left_join_relation(marker_table %>% as_relation, "channel_id", "channel_id") %>%
  left_join_relation(event_table %>% as_relation, "eventId", "eventId")

if(!any(is.na(unlist(spill.list)))) {
  ctx$log(message = "No built-in compensation matrices found.")
  rel_out <- rel_out %>% left_join_relation(spill.list %>% as_relation, list(), list())
}

rel_out %>%
  as_join_operator(list(), list()) %>%
  save_relation(ctx)
