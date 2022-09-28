library(tercen)
library(tercenApi)
library(dplyr, warn.conflicts = FALSE)
library(flowCore)

read_fcs <- function(filename, use.comp, csv.comp, which.lines, separator) {
  
  data_fcs <- read.FCS(
    filename,
    transformation = FALSE,
    which.lines = NULL,
    dataset = 2,
    emptyValue = FALSE,
    ignore.text.offset = TRUE
  )
  
  if(!is.null(which.lines)) {
    nr <- nrow(data_fcs)
    idx <- sample(x = nr, size = min(which.lines, nr))
    data_fcs <- data_fcs[idx, ]
  }

  # Prepare parameters names
  na_desc_idx <- is.na(data_fcs@parameters@data$desc)
  dup_desc_idx <- duplicated(data_fcs@parameters@data$desc)
  data_fcs@parameters@data$desc[na_desc_idx] <- data_fcs@parameters@data$name[na_desc_idx]
  data_fcs@parameters@data$desc[dup_desc_idx] <- data_fcs@parameters@data$name[dup_desc_idx]
  
  desc_parameters <- data_fcs@parameters@data$desc
  
  # Do compensation
  comp.name <- desc_parameters[!(grepl("[FS]S|Time|TIME|Width|Index|Event", desc_parameters))]
  
  if (use.comp) {
    
    spill.matrix <- spillover(data_fcs)[!unlist(lapply(spillover(data_fcs), is.null))]
    
    if(length(spill.matrix) > 1) {
      stop("Multiple compensation matrices found. Compensation cannot be applied.")
    } else if(length(comp.name) != length(colnames(spill.matrix[[1]]))) {
      stop("Different number of columns between FCS file and compensation matrix. Compensation cannot be applied.")
    } else {
      data_fcs = compensate(data_fcs, spill.matrix[[1]])
    }
    
  } else if(!is.null(csv.comp)) {
    
    if (separator == "Comma") {
      matrix.com <- read.csv(csv.comp, header = TRUE, sep = ",", row.names = 1)  
    } else if (separator == "Tab"){
      matrix.com <- read.table(csv.comp, header = TRUE, row.names = 1)  
    } else if  (separator == "Semicolon"){
      matrix.com <- read.csv(csv.comp, header = TRUE, sep = ";", row.names = 1) 
    }
    
    if (length(comp.name) == length(colnames(matrix.com))) {
      colnames(matrix.com) <- comp.name
      data_fcs <- compensate(data_fcs, matrix.com)
    } else {
      stop("Different number of columns between FCS file and compensation matrix. Compensation cannot be applied.")
    }
    
  }
  
  data <- as.data.frame(exprs(data_fcs))
  col_names <- colnames(data)
  desc_parameters <- ifelse(is.na(desc_parameters), col_names, desc_parameters)
  colnames(data) <- desc_parameters
  
  return.data <- data %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci = as.integer(rep_len(0, nrow(.)))) %>%
    # mutate(dirname = rep_len(dirname(filename), nrow(.))) %>%
    mutate(filename = rep_len(basename(filename), nrow(.)))
  
  return(return.data)
}

ctx <- tercenCtx()

if (!any(ctx$cnames == "documentId"))
  stop("Column factor documentId is required")

comp.use <- ctx$op.value("use.builtin.compensation", as.logical, FALSE)
custom.comp.use <- ctx$op.value("use.custom.compensation", as.logical, FALSE)

# if both options are set to TRUE, custom compensation is used
if (comp.use & custom.comp.use) comp.use <- FALSE

separator <- ctx$op.value("Separator", as.character, "Comma")
which.lines <- ctx$op.value("which.lines", as.double, -1)
if(which.lines == -1) which.lines <- NULL

# 1. Extract files
df <- ctx$cselect()
docId <- df$documentId[1]
doc <- ctx$client$fileService$get(docId)
filename_tmp <- tempfile()
writeBin(ctx$client$fileService$download(docId), filename_tmp)
on.exit(unlink(filename_tmp))

# unzip if archive
if (length(grep(".zip", doc$name)) > 0) {
  tmpdir <- tempdir()
  unzip(filename_tmp, exdir = tmpdir)
  f.names <- list.files(
    tmpdir,
    full.names = TRUE,
    pattern = "(\\.fcs$|\\.lmd$|\\.LMD$)",
    ignore.case = TRUE,
    recursive = TRUE
  )
  if (custom.comp.use){
    c.names <- list.files(
      tmpdir,
      full.names = TRUE,
      pattern = "(\\comp.txt$|\\comp.csv$|\\comp.tsv$)",
      ignore.case = TRUE,
      recursive = TRUE
    )
    c.names <- c.names[!grepl("renv", c.names)]
    
    if (length(c.names) != 1) {
      stop("None or more than one CSV compensation matrix found.")
    }
    
  } else {
    c.names <- NULL
  }
} else {
  f.names <- filename_tmp
  c.names <- NULL
}

assign("actual", 0, envir = .GlobalEnv)
task = ctx$task

# 2. Read and convert FCS files
f.names %>%
  lapply(function(filename) {
    
    data = read_fcs(filename, comp.use, c.names, which.lines, separator)
    
    if (!is.null(task)) {
      # task is null when run from RStudio
      actual = get("actual",  envir = .GlobalEnv) + 1
      assign("actual", actual, envir = .GlobalEnv)
      evt = TaskProgressEvent$new()
      evt$taskId = task$id
      evt$total = length(f.names)
      evt$actual = actual
      evt$message = paste0('Processing FCS file: ' , filename)
      ctx$client$eventService$sendChannel(task$channelId, evt)
    } else {
      cat('Processing FCS file: ' , filename)
    }
    data
  }) %>%
  bind_rows() %>%
  ctx$addNamespace() %>%
  ctx$save()
