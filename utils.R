download_files <- function(ctx) {
  df <- ctx$cselect()
  if(!"documentId" %in% colnames(df)) stop("documentId factor needs to be projected onto columns.")
  docId <- df$documentId[1]
  doc <- ctx$client$fileService$get(docId)
  filename_tmp <- tempfile(fileext = paste0(".", tools::file_ext(doc$name)))
  writeBin(ctx$client$fileService$download(docId), filename_tmp)
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
    c.names <- list.files(
      tmpdir,
      full.names = TRUE,
      pattern = "(\\comp.txt$|\\comp.csv$|\\comp.tsv$)",
      ignore.case = TRUE,
      recursive = TRUE
    )
    c.names <- c.names[!grepl("renv", c.names)]
    
    if (length(c.names) > 1) {
      stop("More than one custom compensation matrix found.")
    } else if (length(c.names) == 0) {
      c.names <- NA
    }
  } else {
    f.names <- files$filename_tmp
    c.names <- NA
  }
  return(data.frame(f.names = f.names, c.names = c.names))
}

get_fcs <- function(filename, which.lines) {
  
  data_fcs <- suppressWarnings(read.FCS(
    filename,
    transformation = FALSE,
    which.lines = NULL,
    dataset = 2,
    emptyValue = FALSE,
    ignore.text.offset = TRUE
  ))
  
  if(!is.null(which.lines)) {
    nr <- nrow(data_fcs)
    idx <- sample(x = nr, size = min(which.lines, nr))
    data_fcs <- data_fcs[idx, ]
  }
  
  return(data_fcs)
}

get_spill_matrix <- function(data_fcs, csv.comp, separator) {
  
  if(is.na(csv.comp)) {
    
    spill <- try(spillover(data_fcs), silent = TRUE)
    
    if(inherits(spill, "try-error")) {
      
      spill.matrix <- NA
      
    } else {
      
      spill.matrix <- spillover(data_fcs)[!unlist(lapply(spillover(data_fcs), is.null))]
    
      if(length(spill.matrix) > 1) {
        
        stop("Multiple compensation matrices found. Compensation cannot be applied.")
      
      } else {
        
        spill.matrix <- spill.matrix[[1]]
        
      }
      
    }
  } else {
    if (separator == "Comma") {
      spill.matrix <- read.csv(csv.comp, header = TRUE, sep = ",", row.names = TRUE)  
    } else if (separator == "Tab"){
      spill.matrix <- read.table(csv.comp, header = TRUE, row.names = TRUE)
    } else if  (separator == "Semicolon"){
      spill.matrix <- read.csv(csv.comp, header = TRUE, sep = ";", row.names = TRUE) 
    }
  }
  
  return(spill.matrix)
}

process_fcs <- function(data_fcs, use.descriptions) {
  
  # Prepare parameters names
  na_desc_idx <- is.na(data_fcs@parameters@data$desc)
  dup_desc_idx <- duplicated(data_fcs@parameters@data$desc)
  data_fcs@parameters@data$desc[na_desc_idx] <- data_fcs@parameters@data$name[na_desc_idx]
  data_fcs@parameters@data$desc[dup_desc_idx] <- data_fcs@parameters@data$name[dup_desc_idx]
  
  desc_parameters <- data_fcs@parameters@data$desc
  
  data <- as.data.frame(exprs(data_fcs))
  col_names <- colnames(data)
  if(use.descriptions) {
    desc_parameters <- ifelse(is.na(desc_parameters), col_names, desc_parameters)
    colnames(data) <- desc_parameters
  }
  
  fcs.data <- data %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci = as.integer(rep_len(0, nrow(.)))) %>%
    mutate(filename = rep_len(basename(data_fcs@description$FILENAME), nrow(.)))
  
  return(fcs.data)
}

serialize_comp <- function (df, object, object_name, ctx) 
{
  df$.object <- object_name
  columnTable <- ctx$cselect() %>% mutate(.ci = 0:(nrow(.) - 
                                                     1))
  leftTable <- data.frame(df) %>% ctx$addNamespace() %>% left_join(columnTable, 
                                                                   by = ".ci") %>% select(-.ci) %>% tercen::dataframe.as.table()
  leftTable$properties$name = "left"
  leftRelation <- SimpleRelation$new()
  leftRelation$id <- leftTable$properties$name
  rightTable <- data.frame(compensation_matrices = object_name, .base64.serialized.r.model = c(serialize_to_string(object))) %>% 
    ctx$addNamespace() %>% tercen::dataframe.as.table()
  rightTable$properties$name <- "right"
  rightRelation <- SimpleRelation$new()
  rightRelation$id <- rightTable$properties$name
  pair <- ColumnPair$new()
  pair$lColumns <- list(".object")
  pair$rColumns = list(rightTable$columns[[1]]$name)
  join.model = JoinOperator$new()
  join.model$rightRelation = rightRelation
  join.model$leftPair = pair
  compositeRelation = CompositeRelation$new()
  compositeRelation$id = "compositeRelation"
  compositeRelation$mainRelation = leftRelation
  compositeRelation$joinOperators = list(join.model)
  pair_2 <- ColumnPair$new()
  pair_2$lColumns <- unname(ctx$cnames)
  pair_2$rColumns = unname(ctx$cnames)
  join = JoinOperator$new()
  join$rightRelation = compositeRelation
  join$leftPair = pair_2
  result = OperatorResult$new()
  result$tables = list(leftTable, rightTable)
  result$joinOperators = list(join)
  return(result)
}

serialize_to_string <- function (object) 
{
  if (!inherits(object, "list")) 
    object <- list(object)
  str64_list <- sapply(object, function(x) {
    con <- rawConnection(raw(0), "r+")
    saveRDS(x, con)
    str64 <- base64enc::base64encode(rawConnectionValue(con))
    close(con)
    str64
  })
  return((str64_list))
}
