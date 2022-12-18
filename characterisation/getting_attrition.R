
# server    <- Sys.getenv("SERVER_Long_test")
# server_dbi<- Sys.getenv("SERVER_DBI_Long_test")
# user      <- Sys.getenv("DB_USER_Long_test")
# password  <- Sys.getenv("DB_PASSWORD_Long_test")
# port      <- Sys.getenv("DB_PORT_Long_test") 
# host      <- Sys.getenv("DB_HOST_Long_test") 
# 
# targetDialect              <- "postgresql"
# cdm_database_schema        <- "omop21t2_test" 
# vocabulary_database_schema <- "omop21t2_test" 
# 
# # schema to save results in the database
cdm <- cdm_from_con(db, 
             cdm_schema = cdm_database_schema,
             write_schema = write_schema)

cohortSet <- CDMConnector::readCohortSet(here("Cohort_Dx_initial","1_InstantiateCohorts", "Attrition_cohort"))
CDMConnector::generateCohortSet(cdm, cohortSet = cohortSet, 
                                cohortTableName = "er_long_covid_attrition",
                                overwrite = TRUE)

counts_json <- cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == 22) %>% compute()
cdm <- cdm_from_con(db, 
             cdm_schema = cdm_database_schema,
             write_schema = write_schema, 
             cohort_tables = c("er_long_covid_attrition", "er_cohorts_for_longcov"))


targetCohortId <- 1
influenzaId    <- cohorts_ids %>% filter(type=="influenza") %>% select(cohort_definition_id) %>% pull()
cohortName     <- "er_long_covid_attrition"
censoring_cohorts <- "er_cohorts_for_longcov"


influenza_cohort <- cdm$er_cohorts_for_longcov %>%
  filter(cohort_definition_id == influenzaId) %>%
  rename(influenza_date = cohort_start_date) %>%
  select(subject_id, influenza_date) %>%
  compute()
  
cat("-- Instantiating Attrition cohorts --")

generateDenominatorPopulation <- function(cdm,
                                          cohortName,
                                          targetCohortId,
                                          influenzaId,
                                          anyCovidId) {
  target <- cdm[[cohortName]] %>%
    dplyr::filter(.data$cohort_definition_id == .env$targetCohortId) %>%
    distinct() %>%
    compute()

  # if (targetCohortId != anyCovidId) {
  #   anyCovid <- cdm[[cohortName]] %>%
  #     dplyr::filter(.data$cohort_definition_id == .env$anyCovidId) %>%
  #     dplyr::select("subject_id", "any_covid_date" = "cohort_start_date") %>%
  #     dplyr::compute()
  # }

  attrition <- dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "Starting events",
    excluded = NA
  )

  
  # Age >18 years
  target <- target %>%
    left_join(cdm$person %>% select(subject_id = person_id, 
                                    year_of_birth, month_of_birth, day_of_birth)) %>%
    mutate(age = lubridate::year(cohort_start_date) - year_of_birth) %>% 
    filter(age>17) %>% 
    compute()
  
#   if(database_name=="SIDIAP"){
#     # if we have day and month 
#     target <- target %>%
#       mutate(age=floor(as.numeric((cohort_start_date-
#                                     as.Date(paste(year_of_birth,
#                                                month_of_birth,
#                                                day_of_birth, sep="-"))))/365.25)) %>%
#       compute()
#   } else { 
# target <-  target %>%
#   mutate(age = lubridate::year(cohort_start_date) - year_of_birth) %>% 
#   compute()
#   }
  
  target <- target %>%
    filter(age>17) %>%
    compute()
  
    attrition <- rbind(
    attrition,
    dplyr::tibble(
      number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
      reason = "Aged 18 years or older",
      excluded = NA
    )
  )
    
    # 180 days of prior observation
    target <- target %>%
      dplyr::inner_join(
        cdm[["observation_period"]] %>%
          dplyr::select(
            "subject_id" = "person_id", "observation_period_start_date", "observation_period_end_date"
          ),
        by = "subject_id"
      ) %>%
      dplyr::mutate(
        prior_history =
          .data$cohort_start_date - .data$observation_period_start_date
      ) %>%
      dplyr::filter(.data$prior_history >= 180) %>%
    #  dplyr::select(-"observation_period_start_date", -"prior_history") %>%
      dplyr::compute()
    
    attrition <- rbind(
      attrition,
      dplyr::tibble(
        number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
        reason = "180 days of prior history",
        excluded = NA
      )
    )
    
  # No prior target cohort in the previous 42 days
  target <- target %>%
    dplyr::group_by(.data$subject_id) %>%
    dbplyr::window_order(.data$cohort_start_date) %>%
    dplyr::mutate(target_id = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::compute()
  target <- target %>%
    dplyr::left_join(
      target %>%
        dplyr::mutate(target_id = .data$target_id + 1) %>%
        dplyr::select(
          "subject_id",
          "prev_target" = "cohort_start_date",
          "target_id"
        ),
      by = c("subject_id", "target_id")
    ) %>%
    dplyr::filter(
      is.na(.data$prev_target) |
        .data$cohort_start_date - .data$prev_target > 42
    ) %>%
    dplyr::select(-"target_id", -"prev_target") %>%
    dplyr::compute()

  attrition <- rbind(
    attrition,
    dplyr::tibble(
      number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
      reason = "No prior target in the previous 42 days",
      excluded = NA
    )
  )
  
  # No prior influenza infection 
  target <- target %>%
    dplyr::anti_join(
    target %>%
    dplyr::inner_join(
     influenza_cohort,
      by = "subject_id"
    ) %>%
      dplyr::mutate(days= as.numeric(.data$cohort_start_date - .data$influenza_date)) %>%
      dplyr::filter(days<42 & days>=0)
    ) %>%
    dplyr::compute()
  
  attrition <- rbind(
    attrition,
    dplyr::tibble(
      number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
      reason = "No prior influenza infection in the previous 42 days",
      excluded = NA
    )
  )
  
  if (tagetCohortId == testednegativeId) {
    
    # No prior influenza infection 
    target <- target %>%
      dplyr::anti_join(
        target %>%
          dplyr::inner_join(
            influenza,
            by = "subject_id"
          ) %>%
          dplyr::filter(
            .data$cohort_start_date - .data$influenza_date <= 42
          )
      ) %>%
      dplyr::compute()
    
    attrition <- rbind(
      attrition,
      dplyr::tibble(
        number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
        reason = "No prior influenza infection in the previous 42 days",
        excluded = NA
      )
    )
    
  }

  

  attrition[2:4, 3] <- attrition[1:3, 1] - attrition[2:4, 1]

  results <- list()
  results$attrition <- attrition
  results$cohort <- target

  return(results)
}
