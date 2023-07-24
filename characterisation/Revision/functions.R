codesFromCohort <- function(path, cdm) {
  # initial checks
  #checkInput(path = path, cdm = cdm)
  
  # list jsons
  files <- listJsonFromPath(path)
  
  # obtain codelists
  codelist <- list()
  unknown <- 1
  for (k in seq_along(files)) {
    codelist <- c(
      codelist, codesFromOneCohort(codeslist, unknown, files[k], cdm)
    )
  }
  
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

codesFromOneCohort <- function(codes, unknown, file, cdm) {
  json <- RJSONIO::fromJSON(file)[["ConceptSets"]]
  for (k in seq_along(json)) {
    name <- json[[k]][["name"]]
    if (is.null(name)) {
      name <- paste0("unkown concept set ", unknown)
      unknown <- unknown + 1
    } else if (name %in% names(codes)) {
      name <- paste0(name, " (", file, ")")
    }
    concepts <- json[[k]][["expression"]][["items"]]
    excluded <- NULL
    included <- NULL
    for (j in seq_along(concepts)) {
      conceptId <- concepts[[j]][["concept"]][["CONCEPT_ID"]]
      exclude <- concepts[[j]][["isExcluded"]]
      descendants <- concepts[[j]][["includeDescendants"]]
      if (descendants == TRUE) {
        conceptId <- cdm[["concept_ancestor"]] %>%
          dplyr::filter(.data$ancestor_concept_id == .env$conceptId) %>%
          dplyr::pull("descendant_concept_id")
      }
      if (exclude == TRUE) {
        excluded <- c(excluded, conceptId)
      } else {
        included <- c(included, conceptId)
      }
    }
    codes[[name]] <- included[!(included %in% excluded)]
  }
  return(codes)
}

compute2 <- function(x, ...) {
  ref <- CDMConnector::computeQuery(x, ...)
  if ("GeneratedCohortSet" %in% class(x)) {
    class(ref) <- unique("GeneratedCohortSet", class(x))
  }
  attr(ref, "cdm_reference") <- attr(x, "cdm_reference")
  attr(ref, "cohort_set") <- attr(x, "cohort_set")
  attr(ref, "cohort_attrition") <- attr(x, "cohort_attrition")
  attr(ref, "cohort_count") <- attr(x, "cohort_count")
  return(ref)
}

appendCohortAttributes <- function(cohort,
                                   attritionReason = "Qualifying initial records",
                                   cohortSet = cohortSet(cohort)) {
  checkmate::assertClass(cohort, "GeneratedCohortSet")
  cdm <- attr(cohort, "cdm_reference")
  if (!is.null(cohortSet)) {
    checkmate::assertTRUE(all(
      c("cohort_definition_id", "cohort_name") %in% colnames(cohortSet)
    ))
    
    if (!("tbl_sql" %in% class(cohortSet))) {
      DBI::dbWriteTable(
        conn = attr(cdm, "dbcon"),
        name = ref,
        value = cohortSet,
        overwrite = TRUE,
        temporary = TRUE
      )
      cohortSet <- dplyr::tbl(attr(cdm, "dbcon"), ref)
    }
  } else {
    cohortSet <- cohort %>%
      dplyr::select("cohort_definition_id") %>%
      dplyr::distinct() %>%
      dplyr::mutate(
        cohort_name = paste0("cohort_", .data$cohort_definition_id)
      ) %>%
      compute2()
  }
  
  # update cohort_set
  attr(cohort, "cohort_set") <- cohortSet
  
  # update cohort_count
  attr(cohort, "cohort_count") <- cohort %>%
    dplyr::group_by(.data$cohort_definition_id) %>%
    dplyr::summarise(
      number_records = dplyr::n(),
      number_subjects = dplyr::n_distinct(.data$subject_id)
    ) %>%
    dplyr::right_join(
      attr(cohort, "cohort_set") %>% dplyr::select("cohort_definition_id"),
      by = "cohort_definition_id"
    ) %>%
    dplyr::mutate(
      number_records = dplyr::if_else(
        is.na(.data$number_records), 0, .data$number_records
      ), number_subjects = dplyr::if_else(
        is.na(.data$number_subjects), 0, .data$number_subjects
      )
    ) %>%
    compute2()
  
  # new line of attrition
  attrition <- attr(cohort, "cohort_count") %>%
    dplyr::mutate(
      reason = .env$attritionReason,
      number_records = dplyr::if_else(
        is.na(.data$number_records), 0, .data$number_records
      ),
      number_subjects = dplyr::if_else(
        is.na(.data$number_subjects), 0, .data$number_subjects
      )
    ) %>%
    compute2()
  
  # append line if already exists
  if (!is.null(attr(cohort, "cohort_attrition"))) {
    reasonId <- attr(cohort, "cohort_attrition") %>%
      dplyr::pull("reason_id") %>%
      max()
    attr(cohort, "cohort_attrition")  <- attr(cohort, "cohort_attrition") %>%
      dplyr::union_all(
        attrition %>%
          dplyr::mutate(reason_id = .env$reasonId) %>%
          dplyr::inner_join(
            attr(cohort, "cohort_attrition") %>%
              dplyr::select(
                "cohort_definition_id", "reason_id",
                "previous_records" = "number_records",
                "previous_subjects" = "number_subjects"
              ),
            by = c("cohort_definition_id", "reason_id")
          ) %>%
          dplyr::mutate(
            reason_id = .env$reasonId + 1, reason = .env$attritionReason,
            excluded_records = .data$previous_records - .data$number_records,
            excluded_subjects = .data$previous_subjects - .data$number_subjects
          ) %>%
          dplyr::select(
            "cohort_definition_id", "number_records", "number_subjects",
            "reason_id", "reason", "excluded_records", "excluded_subjects"
          )
      )
  } else {
    attr(cohort, "cohort_attrition") <- attrition %>%
      dplyr::mutate(
        reason_id = 1, excluded_records = 0, excluded_subjects = 0
      ) %>%
      dplyr::select(
        "cohort_definition_id", "number_records", "number_subjects",
        "reason_id", "reason", "excluded_records", "excluded_subjects"
      )
  }
  attr(cohort, "cohort_attrition") <- attr(cohort, "cohort_attrition") %>%
    compute2()
  
  return(cohort)
}
