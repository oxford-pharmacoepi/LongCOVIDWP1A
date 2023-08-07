codesFromCohort <- function(path, cdm) {
  # initial checks
  #checkInput(path = path, cdm = cdm)
  
  # list jsons
  files <- listJsonFromPath(path)
  
  # obtain codelistTibble
  codelistTibble <- NULL
  unknown <- 1
  for (k in seq_along(files)) {
    codelistTibble <- codelistTibble %>%
      dplyr::union_all(extractCodes(files[k], unknown))
  }
  
  # obtain descendants
  codelistTibble <- appendDescendants(codelistTibble, cdm)
  
  # exclude
  codelistTibble <- excludeCodes(codelistTibble)
  
  # split into list
  codelist <- tibbleToList(codelistTibble)
  
  # return
  return(codelist)
}

listJsonFromPath <- function(path) {
  if (file.info(path)[["isdir"]]) {
    files <- list.files(path, full.names = TRUE)
  } else {
    files <- path
  }
  files <- files[tools::file_ext(files) == "json"]
  if (length(files) == 0) {
    cli::cli_abort(paste0("No json files find in ", path))
  }
  return(files)
}

extractCodes <- function(file, unknown) {
  json <- RJSONIO::fromJSON(file)[["ConceptSets"]]
  codelistTibble <- NULL
  for (k in seq_along(json)) {
    name <- json[[k]][["name"]]
    if (is.null(name)) {
      name <- paste0("unkown concept set ", unknown)
      unknown <- unknown + 1
    } else if (name %in% names(codes)) {
      basefile <- basename(file)
      name <- paste0(name, " (", substr(basefile, 1, nchar(basefile) - 5), ")")
    }
    concepts <- json[[k]][["expression"]][["items"]]
    conceptId <- NULL
    includeDescendants <- NULL
    isExcluded <- NULL
    for (j in seq_along(concepts)) {
      conceptId <- c(conceptId, concepts[[j]][["concept"]][["CONCEPT_ID"]])
      exc <- concepts[[j]][["isExcluded"]]
      isExcluded <- c(
        isExcluded, ifelse(is.null(exc), FALSE, exc)
      )
      incD <- concepts[[j]][["includeDescendants"]]
      includeDescendants <- c(
        includeDescendants, ifelse(is.null(incD), FALSE, incD)
      )
    }
    codelistTibble <- codelistTibble %>%
      dplyr::union_all(dplyr::tibble(
        codelist_name = name, concept_id = conceptId, 
        include_descendants = includeDescendants, is_excluded = isExcluded
      ))
  }
  return(codelistTibble)
}

appendDescendants <- function(codelistTibble, cdm) {
  cdm[["concept_ancestor"]] %>%
    dplyr::select("ancestor_concept_id", "descendant_concept_id") %>%
    dplyr::inner_join(
      codelistTibble %>%
        dplyr::filter(.data$include_descendants == TRUE) %>%
        dplyr::rename("ancestor_concept_id" = "concept_id"),
      by = "ancestor_concept_id",
      copy = TRUE
    ) %>%
    dplyr::collect() %>%
    dplyr::select(-"ancestor_concept_id") %>%
    dplyr::rename("concept_id" = "descendant_concept_id") %>%
    dplyr::union_all(
      codelistTibble %>%
        dplyr::filter(.data$include_descendants == FALSE) 
    ) %>%
    dplyr::select(-"include_descendants")
}

excludeCodes <- function(codelistTibble) {
  codelistTibble %>%
    dplyr::filter(.data$is_excluded == FALSE) %>%
    dplyr::select(-"is_excluded") %>%
    dplyr::anti_join(
      codelistTibble %>%
        dplyr::filter(.data$is_excluded == TRUE),
      by = c("codelist_name", "concept_id")
    )
}

tibbleToList <- function(codelistTibble) {
  nam <- unique(codelistTibble$codelist_name)
  codelist <- lapply(nam, function(x) {
    codelistTibble %>%
      dplyr::filter(.data$codelist_name == .env$x) %>%
      dplyr::pull("concept_id") %>%
      unique()
  })
  names(codelist) <- nam
  return(codelist)
}
