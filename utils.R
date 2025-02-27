download_files <- function(ctx) {
  df <- ctx$cselect()
  if(!"documentId" %in% colnames(df)) stop("documentId factor needs to be projected onto columns.")
  docId <- df$documentId[1]
  doc <- ctx$client$fileService$get(docId)
  ext <- tools::file_ext(doc$name)
  
  if(ext == "") {
    filename_tmp <- tempfile()
    writeBin(ctx$client$fileService$download(docId), filename_tmp)
    file_mime_type <- system2(
      command = "file",
      args = paste0(" -b --mime-type ", filename_tmp),
      stdout = TRUE
    )
    if(file_mime_type == "application/zip") {
      doc$name <- paste0(doc$name, ".zip")
    }
  } else {
    filename_tmp <- tempfile(fileext = paste0(".", ext))
    writeBin(ctx$client$fileService$download(docId), filename_tmp)
  }

  return(list(filename_tmp = filename_tmp, docname = doc$name))
}

prepare_files <- function(files) {
  if (length(grep(".zip", files$docname)) > 0) {
    tmpdir <- tempdir()
    unzip(files$filename_tmp, exdir = tmpdir)
    f.names <- list.files(
      tmpdir,
      full.names = TRUE,
      pattern = "(\\.fcs$|\\.lmd$|\\.LMD$)",
      ignore.case = TRUE,
      recursive = TRUE
    )
  } else {
    f.names <- files$filename_tmp
  }
  return(data.frame(f.names = f.names))
}

get_fcs <- function(filename, which.lines, truncate_max_range) {
  
  data_fcs <- suppressWarnings(read.FCS(
    filename,
    transformation = FALSE,
    which.lines = NULL,
    dataset = 2,
    emptyValue = FALSE,
    ignore.text.offset = TRUE,
    truncate_max_range = truncate_max_range
  ))
  
  if(!is.null(which.lines)) {
    nr <- nrow(data_fcs)
    idx <- sample(x = nr, size = min(which.lines, nr))
    data_fcs <- data_fcs[idx, ]
  }
  
  return(data_fcs)
}

get_spill_matrix <- function(data_fcs, separator, ctx) {
  spill <- try(spillover(data_fcs), silent = TRUE)

  if(inherits(spill, "try-error")) {
    spill.matrix <- NA
  } else {
    spill.matrix <- spillover(data_fcs)[!unlist(lapply(spillover(data_fcs), is.null))]
    if(length(spill.matrix) > 1) {
      ctx$log("Multiple compensation matrices found. Only the first one will be output.")
    }
    spill.matrix <- spill.matrix[[1]]
    
    if(keyword(data_fcs)$`$CYT` == "IntelliCyt iQue3") {
      if(all(!grepl("\\D", colnames(spill.matrix)))) {
        colnames(spill.matrix) <- colnames(data_fcs)[as.numeric(colnames(spill.matrix))]
      }
    }
    
    if(any(dim(spill.matrix) == 0)) {
      spill.matrix <- NA
    } else {
      spill.matrix <- spill.matrix %>%
        as_tibble() %>%
        # ctx$addNamespace() %>%
        mutate(comp_1 = colnames(.)) %>%
        tidyr::pivot_longer(cols = !matches("comp_1"), names_to = "comp_2", values_to = "comp_value")
    }
  }
  
  return(spill.matrix)
}

process_fcs <- function(data_fcs, do.gather, ungather_pattern) {
  
  # Prepare parameters names
  na_desc_idx <- is.na(data_fcs@parameters@data$desc)
  dup_desc_idx <- duplicated(data_fcs@parameters@data$desc)
  data_fcs@parameters@data$desc[na_desc_idx] <- data_fcs@parameters@data$name[na_desc_idx]
  data_fcs@parameters@data$desc[dup_desc_idx] <- data_fcs@parameters@data$name[dup_desc_idx]
  
  desc_parameters <- data_fcs@parameters@data$desc
  
  data <- as.data.frame(exprs(data_fcs))
  col_names <- colnames(data)
  
  condx <- grepl(ungather_pattern, col_names, ignore.case = TRUE)
  
  desc_parameters <- ifelse(is.na(desc_parameters), col_names, desc_parameters)
  names_map <- tibble(channel_name = colnames(data), channel_description = desc_parameters) %>%
    dplyr::filter(!condx) %>%
    mutate(channel_id = as.double(seq_len(nrow(.)))) %>%
    mutate(filename = rep_len(basename(data_fcs@description$FILENAME), nrow(.)))

  fcs_name = basename(data_fcs@description$FILENAME)
  
  if(do.gather) colnames(data)[!condx] <- names_map$channel_id
  
  fcs.data <- data %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double)
  
  return(list(fcs.data = fcs.data, names_map = names_map, fcs_name = fcs_name))
}

upload_df <- function(df, ctx, folder_name, prefix, suffix) {
  project <- ctx$client$projectService$get(ctx$schema$projectId)
  folder  <- ctx$client$folderService$getOrCreate(project$id, folder_name)
  
  tbl = tercen::dataframe.as.table(df)
  bytes = memCompress(teRcenHttp::to_tson(tbl$toTson()),
                      type = 'gzip')
  
  fileDoc = FileDocument$new()
  fileDoc$name = paste0(prefix, suffix)
  fileDoc$projectId = project$id
  fileDoc$acl$owner = project$acl$owner
  fileDoc$metadata$contentEncoding = 'gzip'
  
  if (!is.null(folder)) {
    fileDoc$folderId = folder$id
  }
  
  fileDoc = ctx$client$fileService$upload(fileDoc, bytes)
  
  task = CSVTask$new()
  task$state = InitState$new()
  task$fileDocumentId = fileDoc$id
  task$owner = project$acl$owner
  task$projectId = project$id
  
  task = ctx$client$taskService$create(task)
  ctx$client$taskService$runTask(task$id)
  task = ctx$client$taskService$waitDone(task$id)
  if (inherits(task$state, 'FailedState')){
    stop(task$state$reason)
  }
  
  if (!is.null(folder)) {
    schema = ctx$client$tableSchemaService$get(task$schemaId)
    schema$folderId = folder$id
    ctx$client$tableSchemaService$update(schema)
  }
  
  return(NULL)
}

