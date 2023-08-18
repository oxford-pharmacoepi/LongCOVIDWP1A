# initial parameters ----
if (cdmName(cdm) == "SIDIAP") {
  wild_period_start <- as.Date("2020-09-01")
  alpha_period_start <- as.Date("2020-12-15")
  delta_period_start <- as.Date("2021-05-15")
  omicron_period_start <- as.Date("2021-12-15")
} else if (cdmName(cdm) == "CPRD AURUM") {
  wild_period_start <- as.Date("2020-09-01")
  alpha_period_start <- as.Date("2020-12-15")
  delta_period_start <- as.Date("2021-05-15")
  omicron_period_start <- as.Date("2021-12-15")
} else {
  stop("use `SIDIAP` or `CPRD AURUM` as database_name please")
}


# create results folder if needed
cohorts.folder <- here::here("results")
if (!file.exists(cohorts.folder)){
  dir.create(cohorts.folder)
}

# create data folder if needed
cohorts.folder <- here::here("data")
if (!file.exists(cohorts.folder)){
  dir.create(cohorts.folder)
}
rm(cohorts.folder)

# Initial Json cohorts ----
cohorts <- readCohortSet(here("Code", "Cohorts", "Index"))
cdm <- generateCohortSet(
  cdm = cdm, cohortSet = cohorts, name = "index", overwrite = TRUE
)
# 
# cohorts <- readCohortSet(here("Code", "Cohorts", "Vaccinated"))  
# cdm <- generateCohortSet(
#   cdm = cdm, cohortSet = cohorts, name = "vaccinated", overwrite = TRUE
# )
# 
# cohorts <- readCohortSet(here("Code", "Cohorts", "Symptoms"))
# cdm <- generateCohortSet(
#   cdm = cdm, cohortSet = cohorts, name = "symptoms", overwrite = TRUE
# )

cdm <- cdmFromCon(
  con = db, 
  cdmSchema = cdm_database_schema, 
  writeSchema = c("schema" = write_schema, "prefix" = write_stem),
  cohortTables = c("index", "vaccinated", "symptoms"),
  cdmName = database_name
)

# attrition ----

getId <- function(cohort, name) {
  cohortSet(cohort) %>% filter(cohort_name == name) %>% pull(cohort_definition_id)
}

# Ids for initial filtering
covid_id <- getId(cdm$index, "any_covid")
negative_id <- getId(cdm$index, "negative_test")
influenza_id <- getId(cdm$index, "influenza")
# symptoms cohorts
symptoms_ids <- cohortSet(cdm$symptoms) %>% pull(cohort_definition_id)

target <- cdm$index %>%
  filter(cohort_definition_id == covid_id) %>%
  addAge(cdm) %>%
  addCohortIntersectDays(
    cdm = cdm, targetCohortTable = "index", targetCohortId = covid_id, 
    nameStyle = "previous_covid", window = c(-Inf, -1), order = "last"
  ) %>%
  addCohortIntersectDays(
    cdm = cdm, targetCohortTable = "index", targetCohortId = influenza_id, 
    nameStyle = "previous_flu", window = c(-Inf, 0), order = "last"
  ) %>%
  addPriorObservation(cdm = cdm, priorObservationName = "prior_history")

attrition <- dplyr::tibble(
  number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
  reason = "Starting events"
)

target <- target %>%
  filter(age >= 18) %>%
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "Aged 18 years or older"
  )
)

target <- target %>%
  filter(prior_history >= 180) %>%
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "180 days of prior history"
  )
)

target <- target %>%
  filter(is.na(previous_covid) | previous_covid < -42) %>%
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "No prior target in the previous 42 days"  )
)

target <- target %>%
  filter(is.na(previous_flu) | previous_flu < -42) %>%
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "No prior influenza infection in the previous 42 days"
  )
)

attrition <- attrition %>%
  mutate(cohort = "COVID-19 infection")

# tested negative --

target <- cdm$index %>%
  filter(cohort_definition_id == negative_id) %>%
  addAge(cdm) %>%
  addCohortIntersectDays(
    cdm = cdm, targetCohortTable = "index", targetCohortId = negative_id, 
    nameStyle = "previous_test_negative", window = c(-Inf, -1), order = "last"
  ) %>%
  addCohortIntersectDays(
    cdm = cdm, targetCohortTable = "index", targetCohortId = influenza_id, 
    nameStyle = "previous_flu", window = c(-Inf, 0), order = "last"
  ) %>%
  addCohortIntersectFlag(
    cdm = cdm, targetCohortTable = "index", targetCohortId = covid_id, 
    nameStyle = "prior_covid", window = c(-Inf, 0)
  ) %>%
  addPriorObservation(cdm = cdm, priorObservationName = "prior_history")

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "Starting events",
    cohort = "Tested negative"
  )
)

target <- target %>%
  filter(age >= 18) %>%
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "Aged 18 years or older",
    cohort = "Tested negative"
  )
)

target <- target %>%
  filter(prior_history >= 180) %>%
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "180 days of prior history",
    cohort = "Tested negative"
  )
)

## add prior covid diagnoses
target <- target %>%
  filter(prior_covid == 0)%>% 
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "No prior COVID-19 infection",
    cohort = "Tested negative"
  )
)


target <- target %>%
  filter(is.na(previous_flu) | previous_flu < -42) %>%
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "No prior influenza infection in the previous 42 days",
    cohort = "Tested negative"
  )
)

target <- target %>%
  filter(is.na(previous_test_negative) | previous_test_negative < -42) %>%
  computeQuery()

attrition <- rbind(
  attrition,
  dplyr::tibble(
    number_observations = target %>% dplyr::tally() %>% dplyr::pull(),
    reason = "No prior target in the previous 42 days",
    cohort = "Tested negative"
  )
)

# get final cohorts -----

generate_tested_cohorts <- function(index_id, cohort_ids, days_censor_covid){
  # get the cohort of interest
  base_cohort <- cdm$index %>%
    filter(cohort_definition_id == index_id) %>%
    group_by(subject_id) %>% 
    arrange(cohort_start_date) %>%
    mutate(seq=row_number()) %>% 
    ungroup() %>%
    computeQuery() %>%
    arrange() %>%
    addFutureObservation(cdm = cdm) %>%
    addIntersect(
      cdm = cdm, tableName = "death", targetStartDate = "death_date",
      targetEndDate = NULL, value = "days", nameStyle = "death"
    ) %>%
    addCohortIntersectDays(
      cdm = cdm, targetCohortTable = "index", targetCohortId = covid_id,
      window = c(1, Inf), nameStyle = "covid"
    ) %>%
    addCohortIntersectDays(
      cdm = cdm, targetCohortTable = "index", targetCohortId = influenza_id,
      window = c(days_censor_covid, Inf), nameStyle = "influenza"
    ) %>%
    mutate(
      covid_end_date = covid_end_date - cohort_start_date,
      influenza = influenza - 1,
      covid = covid - 1,
      one_year = 365
    ) %>%
    mutate(follow_up_days = pmin(
      future_observation, 
      death, 
      covid, 
      influenza, 
      one_year,
      covid_end_date,
      na.rm = F
    )) %>%
    mutate(reason_censoring = case_when(
      follow_up_days == covid ~ "COVID-19",
      follow_up_days == influenza ~ "influenza",
      follow_up_days == death ~"death",
      follow_up_days == one_year ~ "one year of  follow_up",
      follow_up_days == covid_end_date ~ "End of COVID-19 testing",
      TRUE ~ "end of data collection or exit from database"
    )) %>%
    mutate(cohort_end_date = as.Date(!!dateadd("cohort_start_date", "follow_up_days"))) %>%
    select(cohort_definition_id, subject_id, cohort_start_date, cohort_end_date, seq, follow_up_days, reason_censoring) %>%
    computeQuery()
  
  # exclusion table
  exclusion_table <- tibble(
    N_current = base_cohort %>% tally() %>% pull(), 
    exclusion_reason = "initial pop",
    cohort_definition_id = cohort_ids[1]
  )
  # keep only infections after covid start date - defined in Code to Run
  base_cohort <- base_cohort %>%
    filter(cohort_start_date>=covid_start_date) %>%
    computeQuery()
  
  exclusion_table<-rbind(exclusion_table,
                         tibble(N_current=base_cohort %>%tally()%>%collect()%>%pull(), 
                                exclusion_reason=paste0("Tests prior to ", covid_start_date),
                                cohort_definition_id = cohort_ids[1]
                         ))
  
  # keep only records with at least 120 days of follow-up
  excluded_less_120 <- base_cohort %>%
    filter(follow_up_days <120) %>%
    computeQuery()
  # get the reasons for exclusion due to <120d follow-up - we'll save that
  reason_exclusion <-  excluded_less_120 %>%
    group_by(reason_censoring) %>%
    tally()%>%
    collect()
  
  base_cohort <- base_cohort %>%
    filter(!(follow_up_days<120))%>%
    select(-follow_up_days, -reason_censoring) %>%
    computeQuery()
  
  exclusion_table<-rbind(exclusion_table,
                         tibble(N_current=base_cohort %>%tally()%>%collect()%>%pull(), 
                                exclusion_reason="Less than 120 days of follow-up",
                                cohort_definition_id = cohort_ids[1]
                         ))
  
  ### First COVID-19 infection  
  first_infection <- base_cohort   %>%
    filter(seq==1) %>% 
    select(-seq) %>%
    # we change cohort definition id 
    mutate(cohort_definition_id=!!as.integer(cohort_ids[2])) %>%
    computeQuery() 
  
  # check we have one row per person
  # first_infection %>%tally()    
  # first_infection %>% select(subject_id)%>% distinct() %>%tally() 
  exclusion_table<-rbind(exclusion_table,
                         tibble(N_current=first_infection %>%tally()%>%collect()%>%pull(), 
                                exclusion_reason="Population included",
                                cohort_definition_id = cohort_ids[2]
                         ))
  
  ### Reinfections
  reinfection <- base_cohort  %>%
    filter(seq!=1)%>% 
    select(-seq) %>%
    # we change cohort definition id so that is has its own id
    mutate(cohort_definition_id=!!as.integer(cohort_ids[3])) %>%
    computeQuery()
  
  exclusion_table<-rbind(exclusion_table,
                         tibble(N_current=reinfection %>%tally()%>%collect()%>%pull(), 
                                exclusion_reason="Population included",
                                cohort_definition_id = cohort_ids[3]
                         ))
  # get rid of seq in covid infection
  base_cohort <- base_cohort %>%
    select(-seq) %>%
    mutate(cohort_definition_id= !!as.integer(cohort_ids[1]))%>%
    computeQuery()
  
  result <- list(base_cohort, first_infection, reinfection, exclusion_table, reason_exclusion) 
  result
} 

confirmed_infections <- generate_tested_cohorts(
  index_id = covid_id,
  cohort_ids = c(1, 2, 3),
  days_censor_covid = 42
)
covid_infection <- confirmed_infections[[1]]
first_infection <- confirmed_infections[[2]]
reinfections    <- confirmed_infections[[3]]
## save these cohorts into the database
### PCR and antigen negative _all events 
# id of interest
negative_infections <- generate_tested_cohorts(
  index_id = negative_id,
  cohort_ids = c(4, 5, 6),
  days_censor_covid = 0
)

tested_negative_all <- negative_infections[[1]]
tested_negative_earliest <- negative_infections[[2]]

cdm$final <- covid_infection %>%
  union_all(first_infection) %>%
  union_all(reinfections) %>%
  union_all(tested_negative_all) %>%
  union_all(tested_negative_earliest) %>%
  computeQuery()

#get exclusion table and save ---
exclusion_table <- rbind(confirmed_infections[[4]], negative_infections[[4]]) %>% 
  filter(cohort_definition_id != 6)

exclusion_table <- exclusion_table %>%
  mutate(cohort = ifelse(cohort_definition_id == 1, "COVID-19 infection",
                         ifelse(cohort_definition_id == 2, "First infection",
                                ifelse(cohort_definition_id == 3, "Reinfections",
                                       ifelse(cohort_definition_id == 4, "Tested negative",
                                              ifelse(cohort_definition_id == 5, "First test negative", NA
                                              )))))) %>%
  select(number_observations = N_current,
         reason = exclusion_reason,
         cohort) %>%
  filter(reason!= "initial pop")

attrition <- rbind(attrition, exclusion_table)
# save also reason for censoring
excluded_shorter_follow_up <- rbind(confirmed_infections[[5]] %>% mutate(cohort= "COVID-19"),
                                    negative_infections[[5]]%>% mutate(cohort= "Negative"))

write.csv(attrition,here(paste0("results/exclusion_table_", database_name,".csv")), row.names = FALSE)
write.csv(excluded_shorter_follow_up, here(paste0("results/exclusions_followup_", database_name, ".csv")), row.names = FALSE)



# long covid cohorts ----

### parameters for loop
window_longCov <- c(28,90)

excluded_list   <- list()
denominators_df <- tibble()
## parameters  For testing the loop/function
#symptoms_ids <- c(1,2,3)  # for testing just 3 symptoms
#    i <- 2 # for testing - testing with only one symptom id
#    j <- 1 # for testing - washout window
# cohort <- reinfections

## function including loop to generate all cohorts
symptom_cohorts <- cdm$symptoms %>%
  rename(
    symptom_defintion_id = cohort_definition_id,
    symptom_date = cohort_start_date
  ) %>%
  select(-cohort_end_date)

creating_symptom_cohorts <- function(cohort){
  new_cohort <- cohort %>% filter(cohort_definition_id == 0)
  for (symptom_id in symptoms_ids){
    message(paste0("working on ", symptom_id))
    excluded <- tibble()
    
    # we get our symptom of interest cohort
    symptom <- symptom_cohorts %>%
      filter(symptom_defintion_id==symptom_id) %>%
      computeQuery()
    # we run everything using two different washout windows
    for (j in 1:length(window_longCov)){
      message(paste0("working on ", window_longCov[j]))
      window <- window_longCov[j]
      # we first clean symptoms considering covid infection date
      symptom_covid <- symptom %>%
        inner_join(cohort, by = "subject_id")%>%
        # we keep only symptoms recorded between 180 days prior to the index date and the cohort end date
        filter(!(symptom_date<cohort_start_date-lubridate::days(180))) %>%  
        filter(symptom_date<=cohort_end_date) %>%
        mutate(days_symptom_onset= symptom_date - cohort_start_date) %>% 
        distinct() %>%
        computeQuery()
      # we find the people we need to exclude (symptom within the washout window) -
      # it's people with a negative number for days since symptom onset
      people_to_exclude <- symptom_covid %>%
        filter(days_symptom_onset<0) %>%
        select(subject_id, cohort_start_date) %>%
        computeQuery()
      ## exclude considering both id & cohort_start_date (someone with more than one infection can be included for one infection but not the other)
      symptom_covid <-symptom_covid %>%
        anti_join(people_to_exclude, by = c("subject_id", "cohort_start_date")) %>%
        # exclude rows with symptoms recorded between index date and window (90 or 28 days)
        filter(!(symptom_date< cohort_start_date+lubridate::days(window))) %>%
        # keep only symptoms recorded up to 180 days afeter cohort start date
        #  filter(symptom_date < cohort_start_date + days(180)) %>%
        distinct() %>%
        computeQuery()
      # since people can have more than one symptom recorded, we just keep the first one
      symptom_covid <- symptom_covid %>%
        group_by(subject_id, cohort_start_date) %>%
        filter(symptom_date == min(symptom_date)) %>%
        ungroup() %>%
        computeQuery()
      
      ### now, we add symptoms to covid infection after excluding those with a symptom within the time window
      working_symptom <- cohort %>%
        mutate(symptom_definition_id = symptom_id) %>%
        anti_join(people_to_exclude) %>%
        left_join(symptom_covid) %>%
        mutate(cohort_definition_id = (cohort_definition_id*10^4)+
                 (symptom_definition_id*10^2)+
                 window) %>% 
        mutate(cohort_end_date = ifelse(is.na(symptom_date), cohort_end_date,
                                        lubridate::as_date(pmin(cohort_end_date, symptom_date)))) %>%
        mutate(follow_up_days = cohort_end_date-cohort_start_date)%>%
        computeQuery()
      
      working_symptom <- working_symptom %>%
        mutate(year  = lubridate::year(cohort_start_date),
               month = lubridate::month(cohort_start_date)) %>%
        mutate(trimester=
                 ifelse(year==2020 & month>8,                                    "Sep-Dec 2020",
                        ifelse(year==2021 & month<5,                                    "Jan-Apr 2021",
                               ifelse(year==2021 & (month==5 | month==6| month==7 | month==8), "May-Aug 2021",
                                      ifelse(year==2021 & month>8,                                    "Sep-Dec 2021",
                                             ifelse(year==2022 & month<5,                                    "Jan-Apr 2022", 
                                                    ifelse(year==2022 & month>=5,                                   "May 2022 or later", 
                                                           NA))))))) %>%
        mutate(wave = if_else(
          cohort_start_date < alpha_period_start,  "wild", 
          if_else(
            cohort_start_date < delta_period_start, "alpha",
            if_else(
              cohort_start_date < omicron_period_start, "delta", "omicron"
            )
          )
        )) %>%
        select(-c(year,month)) %>%
        computeQuery()
      
      
      working_denominator <- tibble(
        # cohort id 
        cohort_id =  working_symptom %>% 
          select(cohort_definition_id)%>% distinct() %>% pull(),
        # number of rows
        n_row = working_symptom %>%tally() %>%pull() ,
        # number of subjects
        n_subject = working_symptom %>% select(subject_id)%>%distinct() %>%tally()%>% pull(),
        # follow-up days
        follow_up_days =  working_symptom %>% 
          select(follow_up_days) %>% 
          collect() %>%
          summarise(sum_days= sum(follow_up_days)) %>%
          pull()
      )
      # get the denominator by trimestre ## just nrows
      working_den_trimester <- working_symptom  %>%
        group_by(trimester) %>%
        tally() %>%
        collect() %>%
        mutate(trimester= paste0("n_", trimester)) %>%
        pivot_wider(names_from= trimester, values_from = n) 
      working_den_wave <- working_symptom  %>%
        group_by(wave) %>%
        tally() %>%
        collect() %>%
        mutate(wave = paste0("n_", wave)) %>%
        pivot_wider(names_from= wave, values_from = n) 
      
      working_denominator <- working_denominator %>%
        cbind(working_den_trimester) %>%
        cbind(working_den_wave)
      
      denominators_df <- rbind(denominators_df, working_denominator)
      
      # now we keep only people with the symptom- to generate the cohorts
      working_symptom <- working_symptom %>%
        filter(!(is.na(symptom_date))) %>%
        select(cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) %>%
        computeQuery()
      # insert cohort into the database
      new_cohort <- new_cohort %>%
        union_all(working_symptom) %>%
        computeQuery()
      
      people_to_exclude <- people_to_exclude %>%
        collect() %>%
        mutate(cohort_definition_id = working_symptom %>% 
                 select(cohort_definition_id)%>%
                 distinct() %>% 
                 pull())
      
      excluded <- rbind(excluded, people_to_exclude)
      
    }
    excluded_list[[symptom_id]] <- excluded
    denominators_df
  }
  result <- list(denominators_df, excluded_list)
  attr(new_cohort, "result") <- result
  return(new_cohort)
}

# Covid cohorts & control cohorts 
cdm$long_pos <- creating_symptom_cohorts(cohort = covid_infection)
cdm$long_neg <- creating_symptom_cohorts(cohort = tested_negative_all)
covid_infection_symp <- attr(cdm$long_pos, "result")
tested_negative_all_symp <- attr(cdm$long_neg, "result")
denominator <- rbind(covid_infection_symp[[1]], tested_negative_all_symp[[1]])
# save  denominator table
write.csv(denominator, here(paste0("results/denominator_each_symptom_", database_name, ".csv")),row.names = FALSE)

# generate any symptom cohorts 
{
cdm$long_pos <- cdm$long_pos %>%
  union_all(
    cdm$long_pos %>%
      filter(substr(as.character(cohort_definition_id), 4, 5) == "28") %>%
      mutate(cohort_definition_id = 10028) %>%
      group_by(cohort_definition_id, subject_id, cohort_start_date) %>%
      summarise(cohort_end_date = min(cohort_end_date), .groups = "drop")
  ) %>%
  union_all(
    cdm$long_pos %>%
      filter(substr(as.character(cohort_definition_id), 4, 5) == "90") %>%
      mutate(cohort_definition_id = 10090) %>%
      group_by(cohort_definition_id, subject_id, cohort_start_date) %>%
      summarise(cohort_end_date = min(cohort_end_date), .groups = "drop")
  ) %>%
  computeQuery()

cdm$long_neg <- cdm$long_neg %>%
  union_all(
    cdm$long_neg %>%
      filter(substr(as.character(cohort_definition_id), 4, 5) == "28") %>%
      mutate(cohort_definition_id = 40028) %>%
      group_by(cohort_definition_id, subject_id, cohort_start_date) %>%
      summarise(cohort_end_date = min(cohort_end_date), .groups = "drop")
  ) %>%
  union_all(
    cdm$long_neg %>%
      filter(substr(as.character(cohort_definition_id), 4, 5) == "90") %>%
      mutate(cohort_definition_id = 40090) %>%
      group_by(cohort_definition_id, subject_id, cohort_start_date) %>%
      summarise(cohort_end_date = min(cohort_end_date), .groups = "drop")
  ) %>%
  computeQuery()
cdm$long <- cdm$long_neg %>% union_all(cdm$long_pos) %>% computeQuery()
}

# characterisation ----

# prepare covariates
covariates <- tibble(
  covariate_name = c(
    "autoimmune_disease", "asthma", "malignant_neoplastic_disease", 
    "diabetes", "heart_disease", "gypertensive_disease", "renal_impairment",
    "copd", "dementia"
  ),
  ancestor_concept_id = c(
    434621, 317009, 443392, 201820, 321588, 316866, 4030518, 255573, 4182210
  )
)
conditions <- cdm$condition_occurrence %>%
  rename("subject_id" = "person_id") %>%
  inner_join(
    cdm$final %>% 
      select(subject_id, cohort_start_date) %>% 
      distinct(),
    by = "subject_id"
  ) %>%
  filter(condition_start_date <= cohort_start_date) %>%
  inner_join(
    cdm$concept_ancestor %>%
      inner_join(covariates, by = "ancestor_concept_id", copy = TRUE) %>%
      select(
        condition_concept_id = descendant_concept_id, covariate_name
      ) %>%
      computeQuery(),
    by = "condition_concept_id"
  ) %>%
  select(subject_id, cohort_start_date, covariate_name) %>%
  distinct() %>% 
  mutate(value = 1) %>%
  pivot_wider(names_from = covariate_name, values_from = value) %>%
  computeQuery()

# add covariates
baseline_characteristics <- cdm$final %>%
  mutate(index_id = if_else(cohort_definition_id < 4, 1, 4)) %>%
  addDemographics(futureObservation = FALSE, priorObservation = FALSE) %>%
  mutate(
    age_gr2 = if_else(age <= 34, "<=34", if_else(age <= 49, "35-49", if_else(age <= 64, "50-64", if_else(age <= 79, "65-79", ">=80")))),
    age_gr5 = 5*floor(age/5)
  ) %>%
  mutate(
    age_gr5 = if_else(age < 20, "<20", if_else(age >= 100, ">=100", paste0(age_gr5, "-", age_gr5+4)))
  ) %>%
  addCohortIntersectCount(targetCohortTable = "vaccinated", targetCohortId = getId(cdm$vaccinated, "any_vaccine"), nameStyle = "vaccination_status") %>%
  mutate(vaccination_status = case_when(
    vaccination_status == 0 ~ "Non vaccinated",
    vaccination_status == 1 ~ "First dose vaccination",
    vaccination_status == 2 ~ "Two doses vaccination",
    TRUE ~ "Booster doses"
  )) %>%
  addCohortIntersectFlag(targetCohortTable = "index", targetCohortId = getId(cdm$index, "positive_pcr"), nameStyle = "pcr", window = c(0, 0)) %>%
  mutate(pcr = if_else(cohort_definition_id %in% c(4, 5), 1, pcr)) %>%
  left_join(conditions, by = c("subject_id", "cohort_start_date")) %>%
  mutate(across(covariates$covariate_name, ~ if_else(is.na(.x), 0, .x))) %>%
  mutate(
    year  = lubridate::year(cohort_start_date),
    month = lubridate::month(cohort_start_date),
    week  = lubridate::week(cohort_start_date)
  ) %>%
  mutate(trimester=
           ifelse(year==2020 & month>8,                                    "Sep-Dec 2020",
                  ifelse(year==2021 & month<5,                                    "Jan-Apr 2021",
                         ifelse(year==2021 & (month==5 | month==6| month==7 | month==8), "May-Aug 2021",
                                ifelse(year==2021 & month>8,                                    "Sep-Dec 2021",
                                       ifelse(year==2022 & month<5,                                    "Jan-Apr 2022", 
                                              ifelse(year==2022 & month>=5,                                   "May 2022 or later", 
                                                     NA))))))) %>%
  select(-month) %>%
  mutate(wave = if_else(
    cohort_start_date < alpha_period_start,  "wild", 
    if_else(
      cohort_start_date < delta_period_start, "alpha",
      if_else(
        cohort_start_date < omicron_period_start, "delta", "omicron"
      )
    )
  )) %>%
  computeQuery()

# get symptoms
symptom_names <- cohortSet(cdm$symptoms) %>% 
  pull(cohort_name)
symp_cohort <- cdm$long %>%
  mutate(cohort_definition_id = round(cohort_definition_id)) %>%
  mutate(
    index_id = as.numeric(substr(as.character(cohort_definition_id), 1, 1)),
    symptom_id = as.numeric(substr(as.character(cohort_definition_id), 2, 3)),
    window_days = as.numeric(substr(as.character(cohort_definition_id), 4, 5))
  ) %>%
  select(index_id, subject_id, cohort_start_date, cohort_end_date, symptom_id, window_days) %>%
  mutate(value = if_else(symptom_id == 0, !!datediff("cohort_start_date", "cohort_end_date"), 1)) %>%
  left_join(attr(cdm$symptoms, "cohort_set") %>% rename(symptom_id = cohort_definition_id), by = "symptom_id") %>%
  mutate(cohort_name = if_else(symptom_id == 0, "days_symptom_record", cohort_name)) %>%
  select(-"symptom_id", -"cohort_end_date") %>%
  pivot_wider(names_from = "cohort_name", values_from = "value") %>%
  computeQuery()
for (nam in symptom_names) {
  if (!(nam %in% colnames(symp_cohort))) {
    symp_cohort <- symp_cohort %>%
      mutate(!!nam := 0)
  }
}

# add symptoms
baseline_characteristics_symptoms <- baseline_characteristics %>% 
  mutate(window_days = 28) %>% 
  union_all(baseline_characteristics %>% mutate(window_days = 90)) %>%
  left_join(
    symp_cohort, by = c("index_id", "subject_id", "cohort_start_date", "window_days")
  ) %>%
  mutate(across(all_of(symptom_names), ~ if_else(is.na(.x), 0, .x))) %>%
  mutate(
    all_sympt = abdominal_pain + allergy + altered_smell_taste + anxiety + 
      blurred_vision+chest_pain_or_angina + cognitive_dysfunction_brain_fog + 
      cough + depression_wo_recurrence + dizziness + dyspnea + fatigue_malaise +
      gastrointestinal_issues + headache + intermittent_fever + joint_pain + 
      memory_issues + menstrual_problems + muscle_spasms_and_pain + neuralgia +
      pins_and_needles_sensation + postexertional_fatigue + sleep_disorder +
      tachycardia + tinnitus_hearing_problems
  ) %>%
  collect()

baseline_characteristics_symptoms <- baseline_characteristics_symptoms %>%
  mutate(sex = factor(sex)) %>%
  mutate(age_gr2 = factor(age_gr2, levels = c("<=34", "35-49", "50-64", "65-79", ">=80"))) %>%
  mutate(trimester = factor(trimester, levels = c("Sep-Dec 2020", "Jan-Apr 2021", "May-Aug 2021", "Sep-Dec 2021", "Jan-Apr 2022", "May 2022 or later"))) %>%
  mutate(vaccination_status = factor(vaccination_status, levels = c("Non vaccinated", "First dose vaccination", "Two doses vaccination", "Booster doses" ))) %>%
  mutate(age_gr5 = factor(age_gr5)) %>%
  droplevels() 

baseline_characteristics_symptoms <- baseline_characteristics_symptoms %>%
  rename(
    "hypertension" = "gypertensive_disease",
    "cancer" = "malignant_neoplastic_disease",
    "all_symp" = "all_sympt"
  ) %>%
  mutate(
    total_days_followup = as.numeric(cohort_end_date - cohort_start_date),
    long_covid = if_else(all_symp > 0, 1, 0)
  )

save(baseline_characteristics_symptoms, file = here("data", "baseline_characteristics_symptoms.Rdata"))
  
# Match cohorts ----

table1_data_new_infections <- baseline_characteristics_symptoms %>% 
  filter(cohort_definition_id == 1)
table1_data_first_infection <- baseline_characteristics_symptoms %>% 
  filter(cohort_definition_id == 2)
table1_data_reinfections <- baseline_characteristics_symptoms %>% 
  filter(cohort_definition_id == 3)
table1_data_tested_negative_earliest <- baseline_characteristics_symptoms %>% 
  filter(cohort_definition_id == 5)
table1_data_tested_negative_all <- baseline_characteristics_symptoms %>% 
  filter(cohort_definition_id == 4)
  
# Matching cohorts by week of test  and 5y age band
matching_cohorts <- function(cohort1, cohort2, 
                             name1, name2, ratio, window_id) {
  results_list <- list()
  cohort_1 <- cohort1 %>% filter(window_days == window_id)    # cohort1 is the Exposed cohort
  cohort_2 <- cohort2 %>% filter(window_days == window_id)   # cohort2 is the control cohort
  
  data <- rbind(cohort_1 %>%  mutate(cohort = 1, name = name1) ,
                # comparator cohort
                cohort_2 %>% mutate(cohort =0,   name = name2)) %>%
    mutate(age_gr5 = factor(age_gr5), sex = factor(sex))
    
  
  # Exact matching on week and year of the test, type of test, ang 5y age group
  m.week_year <- matchit(
    cohort  ~ week + year + pcr + age_gr5 + sex, 
    data   = data,
    method = "nearest", 
    exact  = ~ week + year + pcr + age_gr5 + sex,
    ratio  = ratio
  )
  
  # extract results of matching
  sum_matching <- summary(m.week_year)
  # extract matched data
  m.data <- match.data(
    # were matched data are
    object = m.week_year,
    # all for both controls and treated, can be treated or controls
    group = "all",
    # name for the weights - here all weights are 1
    weights = "ps_weights",
    # matched pair membership name
    subclass = "matched_pair",
    # dataset to add new columms
    data = data,
    drop.unmatched = TRUE)
  
  # save this in survey object - this is needed to create later the summary tables using svytableone and tbl_summary
  s.data <- svydesign(ids = ~matched_pair, # clusters
                      data = m.data,
                      weights= ~ps_weights) # weights
  
  results_list <- list(sum_matching, m.data, s.data)
  results_list
}

# 90 days symptoms
m.new_covid_tested_negative_earliest_90 <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  cohort2 = table1_data_tested_negative_earliest, 
  name2 = "Tested negative, earliest",
  ratio = 3,
  window_id = 90
)

m.new_covid_tested_negative_all_90 <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  cohort2 = table1_data_tested_negative_all, 
  name2 = "Tested negative, all",
  ratio = 3,
  window_id = 90
)

m.first_infection_tested_negative_earliest_90 <- matching_cohorts(
  cohort1 = table1_data_first_infection,
  name1 = "First infection",
  cohort2 = table1_data_tested_negative_earliest, 
  name2 = "Tested negative, earliest",
  ratio = 3,
  window_id = 90
)

m.first_infection_reinfections_90 <- matching_cohorts(
  cohort1 = table1_data_reinfections,
  name1 = "Reinfections",
  cohort2 = table1_data_first_infection,
  name2 = "First infection",
  ratio = 3,
  window_id = 90
)

save(m.new_covid_tested_negative_earliest_90,
     m.first_infection_tested_negative_earliest_90,
     m.new_covid_tested_negative_all_90,
     m.first_infection_reinfections_90,
     file = here("data", "table1_data_matched_90.Rdata"))

# 28 days symptoms
m.new_covid_tested_negative_earliest_28 <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  cohort2 = table1_data_tested_negative_earliest, 
  name2 = "Tested negative, earliest",
  ratio = 3,
  window_id = 28
)

m.new_covid_tested_negative_all_28 <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  cohort2 = table1_data_tested_negative_all, 
  name2 = "Tested negative, all",
  ratio = 3,
  window_id = 28
)

m.first_infection_tested_negative_earliest_28 <- matching_cohorts(
  cohort1 = table1_data_first_infection,
  name1 = "First infection",
  cohort2 = table1_data_tested_negative_earliest, 
  name2 = "Tested negative, earliest",
  ratio = 3,
  window_id = 28
)

m.first_infection_reinfections_28 <- matching_cohorts(
  cohort1 = table1_data_reinfections,
  name1 = "Reinfections",
  cohort2 = table1_data_first_infection,
  name2 = "First infection",
  # cohort2
  ratio = 3,
  window_id = 28
)

save(m.new_covid_tested_negative_earliest_28,
     m.first_infection_tested_negative_earliest_28,
     m.new_covid_tested_negative_all_28,
     m.first_infection_reinfections_28,
     file = here("data", "table1_data_matched_28.Rdata"))

# create descriptive tables ----

# functions for numbers
nice.num<-function(x){
  prettyNum(x, big.mark=",", nsmall = 0, scientific = FALSE)}

# variables of interest 

other_factor_vars <- c( "age_gr2",
                        "sex",
                        "trimester",
                        "wave",
                        "vaccination_status",
                        "pcr",
                        #comorbidities
                        "asthma",
                        "autoimmune_disease",
                        "copd",
                        "dementia",
                        "diabetes",
                        "heart_disease",
                        "cancer",
                        "hypertension",
                        "renal_impairment",
                        # long covid + symptoms
                        "long_covid")

factor.vars <- c(other_factor_vars, symptom_names, "all_symp")
rm(other_factor_vars)
# all variables - (includes continous variables)
vars <- c("total_days_followup", "age", factor.vars, "days_symptom_record")

# load matched data 
load(here("data/table1_data_matched_28.Rdata"))
load(here("data/table1_data_matched_90.Rdata"))

# function to create descriptive tables for matched data
get_characteristics_matched <- function(df){
  
  summary_characteristics <-print(svyCreateTableOne(
    vars =  vars,
    factorVars = factor.vars,
    includeNA=F,
    data = df,
    strata=c("name"),
    test = F), 
    showAllLevels=F,
    smd=T,
    nonnormal = vars, #all 
    noSpaces = TRUE,
    printToggle=FALSE)
  
  for(i in 1:2) {
    # tidy up numbers
    cur_column <- summary_characteristics[, i]
    cur_column <- str_extract(cur_column, '[0-9.]+\\b') %>% 
      as.numeric() 
    cur_column <-nice.num(cur_column)
    # add back in
    summary_characteristics[, i] <- str_replace(string=summary_characteristics[, i],
                                                pattern='[0-9.]+\\b', 
                                                replacement=cur_column)    
  }
  # tidy up rownames
  rownames(summary_characteristics)<-str_replace(rownames(summary_characteristics), "age_gr2", "Age, categories")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics), " = 1", "")
  rownames(summary_characteristics)<-str_replace(rownames(summary_characteristics), "a_", "")
  rownames(summary_characteristics)<-str_replace_all(rownames(summary_characteristics) , "_", " ")
  rownames(summary_characteristics)<- str_replace_all(rownames(summary_characteristics) , "all symp", "number of symptoms")
  
  rownames(summary_characteristics)<-str_to_sentence(rownames(summary_characteristics))
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Copd", "COPD")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Altered smell taste", "Altered smell or taste")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Long covid", "Long Covid symptoms")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Fatigue malaise", "Fatigue or malaise")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Muscle spams pain", "Muscle spasms or pain")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Cognitive dysfunction brain fog", "Cognitive dysfunction")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Sex", "Sex, female")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Pcr", "PCR test")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics), "Days symptom record",
                                                  "Days elapsed since index date and record of symptom")
  rownames(summary_characteristics) <- str_replace(rownames(summary_characteristics), "Total days followup",
                                                   "Days of follow-up")
  rownames <- rownames(summary_characteristics)
  # mask counts <5
  summary_characteristics <-apply(summary_characteristics, 
                                  # apply to columns
                                  2, 
                                  function(x) 
                                    ifelse(str_sub(x, 1, 2) %in%  c("0 ", "1 ","2 ", "3 ","4 "),
                                           "<5",x))
  
  rownames(summary_characteristics) <- rownames
  summary_characteristics
}
# function to get all the descriptive tables  
get_descriptive_tables <- function(window_id){ 
  print(paste0("Getting Table 1 -", window_id, "days"))
  # Get table1 data for each cohort of interest
  table1_data_new_infections <- baseline_characteristics_symptoms %>% 
    filter(cohort_definition_id == 1 & window_days == window_id) 
  table1_data_first_infection <- baseline_characteristics_symptoms %>%
    filter(cohort_definition_id == 2 & window_days == window_id)
  table1_data_reinfections <- baseline_characteristics_symptoms %>%
    filter(cohort_definition_id == 3 & window_days == window_id)
  table1_data_tested_negative_earliest <- baseline_characteristics_symptoms %>%
    filter(cohort_definition_id == 5 & window_days == window_id)
  table1_data_tested_negative_all <- baseline_characteristics_symptoms %>%
    filter(cohort_definition_id == 4 & window_days == window_id)
  
  table1.data<-rbind(  
    table1_data_new_infections %>%
      mutate(group = "COVID-19 infection"),
    table1_data_first_infection %>%
      mutate(group = "First infection"),
    table1_data_reinfections  %>%
      mutate(group = "Reinfections"),
    table1_data_tested_negative_earliest  %>%
      mutate(group = "Tested negative, earliest"),
    table1_data_tested_negative_all %>%
      mutate(group = "Tested negative, all"))
  
  summary_characteristics <-print(CreateTableOne(
    vars =  vars,
    factorVars = factor.vars,
    includeNA=F,
    data = table1.data,
    strata=c("group"),
    test = F), 
    showAllLevels=F,
    smd=F,
    nonnormal = vars, #all 
    noSpaces = TRUE,
    printToggle= FALSE)
  
  for(i in 1:5) {
    # tidy up 
    cur_column <- summary_characteristics[, i]
    cur_column <- str_extract(cur_column, '[0-9.]+\\b') %>% 
      as.numeric() 
    cur_column <-nice.num(cur_column)
    # add back in
    summary_characteristics[, i] <- str_replace(string=summary_characteristics[, i],
                                                pattern='[0-9.]+\\b', 
                                                replacement=cur_column)    
  }
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics), "age_gr2", "Age, categories")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics), " = 1", "")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics), "a_", "")
  rownames(summary_characteristics)<- str_replace_all(rownames(summary_characteristics) , "_", " ")
  rownames(summary_characteristics)<- str_replace_all(rownames(summary_characteristics) , "all symp", "number of symptoms")
  rownames(summary_characteristics)<- str_to_sentence(rownames(summary_characteristics))
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Copd", "COPD")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Altered smell taste", "Altered smell or taste")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Long covid", "Long Covid symptoms")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Fatigue malaise", "Fatigue or malaise")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Muscle spams pain", "Muscle spasms or pain")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Cognitive dysfunction brain fog", "Cognitive dysfunction")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Sex", "Sex, female")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics) , "Pcr", "PCR test")
  rownames(summary_characteristics)<- str_replace(rownames(summary_characteristics), "Days symptom record",
                                                  "Days elapsed since index date and record of symptom")
  rownames(summary_characteristics) <- str_replace(rownames(summary_characteristics), "Total days followup",
                                                   "Days of follow-up")
  rownames <- rownames(summary_characteristics)
  # mask counts <5
  summary_characteristics <-apply(summary_characteristics,2,  function(x) 
    ifelse(str_sub(x, 1, 2) %in%  c("0 ", "1 ","2 ", "3 ","4 "),"<5",x))
  rownames(summary_characteristics) <- rownames
  # save Table 1
  write.csv(summary_characteristics, file = here(paste0("results/Table1_", window_id, "days_", database_name, ".csv")))
  
  print(paste0("Getting characteristics for matched cohorts -", window_id, "days"))
  
  # Comparing characteristics of matched infections vs tested negative, earliest
  m.covid_tested_neg_earliest           <-  get(paste0("m.new_covid_tested_negative_earliest_", window_id))[[3]]
  m.first_infection_tested_neg_earliest <-  get(paste0("m.first_infection_tested_negative_earliest_", window_id))[[3]]
  m.covid_tested_neg_all                <-  get(paste0("m.new_covid_tested_negative_all_", window_id))[[3]]
  m.first_infection_reinfections        <-  get(paste0("m.first_infection_reinfections_", window_id))[[3]]
  
  sum_characteristics_covid_negative_earliest <- get_characteristics_matched(df = m.covid_tested_neg_earliest)
  sum_characteristics_first_negative_earliest <- get_characteristics_matched(df = m.first_infection_tested_neg_earliest)
  sum_characteristics_covid_negative_all      <- get_characteristics_matched(df = m.covid_tested_neg_all)
  sum_characteristics_reinfections            <- get_characteristics_matched(df = m.first_infection_reinfections) 
  
  write.csv(sum_characteristics_covid_negative_earliest, 
            file = here(paste0("results/Table_Matched_Infection_Tested_Negative_earliest_", window_id, "days_", database_name, ".csv")))
  write.csv( sum_characteristics_first_negative_earliest, 
             file = here(paste0("results/Table_Matched_First_Infection_Tested_Negative_earliest_", window_id, "days_", database_name, ".csv")))
  write.csv(sum_characteristics_covid_negative_all, 
            file = here(paste0("results/Table_Matched_Infection_Tested_Negative_all_", window_id, "days_", database_name, ".csv")))
  write.csv(sum_characteristics_reinfections, 
            file = here(paste0("results/Table_Matched_First_infection_Reinfections_", window_id, "days_", database_name, ".csv")))
  
  # get proportion of long covid by month - only for new infections
  prop_long_covid <- rbind(table1_data_new_infections %>%
                             mutate(follow_up = ifelse(!(is.na(days_symptom_record)),
                                                       days_symptom_record,
                                                       total_days_followup)) %>%
                             mutate(month_year = format(as.Date(cohort_start_date),"%Y-%m" )) %>%
                             group_by(month_year) %>%
                             summarise(n = n(),
                                       long_covid = sum(long_covid),
                                       follow_up = sum(follow_up)) %>%
                             mutate(prop = long_covid/n*100) %>%
                             mutate(ir = long_covid/follow_up*100000*365) %>%
                             mutate(cohort = "COVID-19 infection") %>%
                             mutate(sex= "Both") %>%
                             mutate(age_gr2 = "Overall"), 
                           # by age group & sex
                           table1_data_new_infections %>%
                             mutate(follow_up = ifelse(!(is.na(days_symptom_record)), 
                                                       days_symptom_record, 
                                                       total_days_followup)) %>%
                             mutate(month_year = format(as.Date(cohort_start_date),"%Y-%m" )) %>%
                             group_by(month_year, age_gr2, sex) %>%
                             summarise(n = n(),
                                       long_covid = sum(long_covid),
                                       follow_up = sum(follow_up)) %>%
                             mutate(prop = long_covid/n*100) %>%
                             mutate(ir = long_covid/follow_up*100000*365) %>%
                             mutate(cohort = "COVID-19 infection"),
                           # for negative tests
                           table1_data_tested_negative_earliest %>%
                             mutate(follow_up = ifelse(!(is.na(days_symptom_record)), 
                                                       days_symptom_record, 
                                                       total_days_followup)) %>%
                             mutate(month_year = format(as.Date(cohort_start_date),"%Y-%m" )) %>%
                             group_by(month_year) %>%
                             summarise(n = n(),
                                       long_covid = sum(long_covid),
                                       follow_up = sum(follow_up)) %>%
                             mutate(prop = long_covid/n*100) %>%
                             mutate(ir = long_covid/follow_up*100000*365) %>%
                             mutate(cohort = "First negative test") %>%
                             mutate(sex = "Both") %>%
                             mutate(age_gr2 = "Overall"), 
                           # by age group % sex
                           table1_data_tested_negative_earliest %>%
                             mutate(follow_up = ifelse(!(is.na(days_symptom_record)),
                                                       days_symptom_record, 
                                                       total_days_followup)) %>%
                             mutate(month_year = format(as.Date(cohort_start_date),"%Y-%m" )) %>%
                             group_by(month_year, age_gr2, sex) %>%
                             summarise(n = n(),
                                       long_covid = sum(long_covid),
                                       follow_up = sum(follow_up)) %>%
                             mutate(prop = long_covid/n*100) %>%
                             mutate(ir = long_covid/follow_up*100000*365) %>%
                             mutate(cohort = "First negative test")) %>%
    mutate(database = database_name) %>%
    # remove rows with <5 counts
    filter(!(long_covid<5)) %>%
    mutate(incidence_start_date = as_date(paste0(month_year, "-01"))) %>%
    select(cohort, 
           n_persons = n,
           n_events = long_covid,
           incidence_start_date,
           person_days = follow_up,
           ir_100000_pys = ir,
           sex = sex,
           age_group = age_gr2,
           database)
  
  prop_long_covid <- cbind(prop_long_covid,
                           pois.exact(prop_long_covid$n_events,
                                      pt = prop_long_covid$person_days,
                                      conf.level = 0.95) %>%
                             mutate(ir_100000_pys_95CI_lower = lower*100000*365,
                                    ir_100000_pys_95CI_upper = upper*100000*365) %>%
                             select(ir_100000_pys_95CI_lower,ir_100000_pys_95CI_upper))
  
  write.csv(prop_long_covid, 
            file = here(paste0("results/Proportion_LongCovid_", window_id, "days_", database_name, ".csv")), row.names = FALSE)
}

get_descriptive_tables(window_id = 90) 
get_descriptive_tables(window_id = 28) 

# function to get RR and AR ----
get_rr <- function(x){
  results_rr <- NULL
  for (k in c("overall", "wild", "alpha", "delta", "omicron")) {
    if (k == "overall") {
      data <- x
    } else {
      data <- x %>%
        filter(wave == k)
    }
    # for each symptom
    for (symptom_id in symptom_names){
      message(paste0("working on ", symptom_id))
      # get number of events & number of people per cohort
      counts_1 <- data %>%
        mutate(symptom =paste0(symptom_id)) %>%
        rename(name_1 = name) %>%
        filter(cohort==1) %>%
        mutate(total_pop_1 = length(subject_id)) %>% # get total pop
        filter((!!sym(symptom_id))==1) %>%
        mutate(events_1= length(subject_id))%>% # get number of people with the symtpom
        select(symptom, name_1, events_1, total_pop_1) %>% 
        distinct()  %>%
        mutate(prop_1 = (events_1/total_pop_1)*100)  # get proportion of people with the symtpmm
      
      counts_2<- data %>%
        mutate(symptom =paste0(symptom_id)) %>%
        rename(name_2 = name) %>%
        filter(cohort==0) %>%
        mutate(total_pop_2 = length(subject_id)) %>% 
        filter((!!sym(symptom_id))==1) %>%
        mutate(events_2= length(subject_id))%>% 
        select(symptom, name_2, events_2, total_pop_2) %>% 
        distinct()  %>%
        mutate(prop_2 = (events_2/total_pop_2)*100) 
      
      data_rr <- counts_1 %>%
        left_join(counts_2) %>%
        mutate(database = database_name) %>%
        mutate(symptom = str_to_sentence(str_replace_all(symptom, "_", " "))) %>%
        mutate(wave = k)
      
      # getting relative risl with 95%Ci
      getting_rr <- fmsb::riskratio(data_rr$events_1,
                                    data_rr$events_2,
                                    data_rr$total_pop_1,
                                    data_rr$total_pop_2,
                                    conf.level = 0.95)
      
      data_rr$relative_risk <- getting_rr$estimate
      data_rr$low_ci <- getting_rr$conf.int[1]
      data_rr$up_ci <- getting_rr$conf.int[2]
      
      # # getting absolute risk difference with 95%C
      # getting_ar <- BinomDiffCI(
      #   data_rr$events_1,   # events exposed
      #   data_rr$total_pop_1,# pop exposed
      #   data_rr$events_2,
      #   data_rr$total_pop_2,
      #   conf.level = 0.95, 
      #   sides = "two.sided",
      #   method = "wald")
      # data_rr$absolute_risk <- getting_ar[1]*100
      # data_rr$ar_low_ci <- getting_ar[2]*100
      # data_rr$ar_up_ci <- getting_ar[3]*100
      
      # keep only RR for events >=5
      data_rr <- data_rr %>% filter(events_1 >=5 & events_2 >=5)
      results_rr <- bind_rows(results_rr, data_rr)      
    }
  }
  return(results_rr)
}
save_rr <- function(window_id){
  m.covid_tested_neg_earliest    <-  get(paste0("m.new_covid_tested_negative_earliest_", window_id))[[2]] 
  m.first_infection_tested_neg_earliest <- get(paste0("m.first_infection_tested_negative_earliest_", window_id))[[2]]
  m.covid_tested_neg_all         <-  get(paste0("m.new_covid_tested_negative_all_", window_id))[[2]]
  m.first_infection_reinfections <-  get(paste0("m.first_infection_reinfections_", window_id))[[2]]
  
  rr_new_covid_tested_negative_earliest <- bind_rows(get_rr(x = m.covid_tested_neg_earliest))
  rr_first_infection_tested_negative_earliest <- bind_rows(get_rr(x = m.first_infection_tested_neg_earliest)) 
  rr_new_covid_tested_negative_all      <- bind_rows(get_rr(x = m.covid_tested_neg_all))
  rr_first_infection_reinfections       <- bind_rows(get_rr(x = m.first_infection_reinfections)) 
  
  write.csv(rr_new_covid_tested_negative_earliest,
            here(paste0("results/RR_new_covid_tested_negative_earliest_", window_id, "days_",
                        database_name, 
                        ".csv")), row.names = FALSE)
  
  write.csv(rr_first_infection_tested_negative_earliest,
            here(paste0("results/RR_first_infection_tested_negative_earliest_", window_id, "days_",
                        database_name, 
                        ".csv")), row.names = FALSE)
  
  write.csv(rr_new_covid_tested_negative_all,
            here(paste0("results/RR_new_covid_tested_negative_all_", window_id, "days_",
                        database_name,
                        ".csv")),row.names = FALSE)
  
  write.csv(rr_first_infection_reinfections,
            here(paste0("results/RR_first_infection_reinfections_", window_id, "days_",
                        database_name,
                        ".csv")), row.names = FALSE)
}

print("Getting RR")

save_rr(window_id = 90)
save_rr(window_id = 28)

# Generate incidence rates  ----

# parameters 
study_start_date <- as.Date(c(covid_start_date, NA)) # needed to get the gen pop denominator 
study_days_prior_history <- 180  # needed for the gen pop denominator
# age and sex stratas, first overal
study_age_stratas <- list(c(18,150))
study_sex_stratas <- "Both"
# get denominators
cdm <- generateDenominatorCohortSet(cdm = cdm,
                                    cohortDateRange = study_start_date,
                                    daysPriorHistory = study_days_prior_history,
                                    ageGroup = study_age_stratas,
                                    sex = study_sex_stratas)
excluded <- cohortAttrition(cdm$denominator)

get_ir <- function(cdm, cohortTableName, cohortId){
  
  outcome <- cdm[[cohortTableName]] %>% 
    filter(cohort_definition_id == cohortId) %>%
    mutate(cohort_end_date = as_date(cohort_start_date + lubridate::days(42))) %>% 
    computeQuery()
  cdm[["outcome"]] <- newGeneratedCohortSet(outcome)
  
  ## get incidence rates
  inc <- estimateIncidence(
    cdm = cdm,
    denominatorTable = "denominator",
    outcomeTable = "outcome",
    completeDatabaseIntervals = TRUE,
    interval = c("months"),
    repeatedEvents = TRUE,
    minCellCount = 5,
    outcomeWashout = 0,
    verbose = TRUE
  ) 
  inc <- inc %>%
    left_join(settings(inc) %>%
                select(analysis_id, denominator_age_group, denominator_sex) %>%
                rename(age_group = denominator_age_group,
                       sex = denominator_sex))
  inc
}

## get IR
covid_genpop <- get_ir(main_cohort_id = new_infection_id) %>% 
  mutate(cohort = "COVID-19 infections")
longCov_genpop_90 <- get_ir(main_cohort_id = paste0(new_infection_id*10^4+long_covid_id*10^2+90)) %>%
  mutate(cohort = "Long COVID-19 (90 days)")
longCov_genpop_28 <- get_ir(main_cohort_id = paste0(new_infection_id*10^4+long_covid_id*10^2+28))%>%
  mutate(cohort = "Long COVID-19 (28 days)")

neg_test_genpop <- get_ir(main_cohort_id = tested_negative_all_id) %>% 
  mutate(cohort = "Tested negative")
longCov_genpop_negative_90 <- get_ir(main_cohort_id = paste0(tested_negative_all_id*10^4+long_covid_id*10^2+90)) %>%
  mutate(cohort = "Long COVID symptoms, tested negative (90 days)")
longCov_genpop_negative_28 <- get_ir(main_cohort_id = paste0(tested_negative_all_id*10^4+long_covid_id*10^2+28)) %>%
  mutate(cohort = "Long COVID-19 symptoms, tested negative (28 days)")

# stratified by age group and sex
study_age_stratas <- list(c(18,34), # only adults for this study
                          c(35,49),
                          c(50,64),
                          c(65,79),
                          c(80,150))
study_sex_stratas <- c("Female", "Male")
cdm$denominator <- generateDenominatorCohortSet(cdm = cdm,
                                                startDate = study_start_date,
                                                daysPriorHistory = study_days_prior_history,
                                                ageGroup = study_age_stratas,
                                                sex = study_sex_stratas)

covid_genpop_strat <- get_ir( main_cohort_id = new_infection_id) %>%
  mutate(cohort = "COVID-19 infections")

longCov_genpop_strat_90 <- get_ir(main_cohort_id = paste0(new_infection_id*10^4+long_covid_id*10^2+90)) %>%
  mutate(cohort = "Long COVID-19 (90 days)")

longCov_genpop_strat_28 <- get_ir( main_cohort_id = paste0(new_infection_id*10^4+long_covid_id*10^2+28)) %>%
  mutate(cohort = "Long COVID-19 (28 days)")

neg_test_genpop_strat <- get_ir(main_cohort_id = tested_negative_all_id) %>% 
  mutate(cohort = "Tested negative")
longCov_genpop_negative_strat_90 <- get_ir(main_cohort_id = paste0(tested_negative_all_id*10^4+long_covid_id*10^2+90)) %>%
  mutate(cohort = "Long COVID symptoms, tested negative (90 days)")
longCov_genpop_negative_strat_28 <- get_ir(main_cohort_id = paste0(tested_negative_all_id*10^4+long_covid_id*10^2+28)) %>%
  mutate(cohort = "Long COVID-19 symptoms, tested negative (28 days)")


# save results 
incidence_estimates <- rbind(covid_genpop, 
                             longCov_genpop_90, 
                             longCov_genpop_28,
                             covid_genpop_strat,
                             longCov_genpop_strat_90, 
                             longCov_genpop_strat_28) %>%
  mutate(database = database_name)

incidence_estimates_negative <- rbind(neg_test_genpop,
                                      neg_test_genpop_strat,
                                      longCov_genpop_negative_90,
                                      longCov_genpop_negative_28,
                                      longCov_genpop_negative_strat_90 ,
                                      longCov_genpop_negative_strat_28 ) %>%
  mutate(database = database_name)

write.csv(incidence_estimates,
          here(paste0("results/Incidence_rates_Covid_LongCov_", 
                      database_name, 
                      ".csv")), row.names = FALSE)
write.csv(incidence_estimates_negative,
          here(paste0("results/Incidence_rates_Negative_LongCov_", 
                      database_name, 
                      ".csv")), row.names = FALSE)


# Generate tables  ----

# save results into a zip folder
file <-c(paste0(here(), "/results"))

zipName <- paste0("Results_", database_name, ".zip")
createZipFile(zipFile = zipName,
              rootFolder= paste0(here(), "/results"),
              files = file)
rm(list = ls())
print("Done!")
print("If all has worked, you should have a Zip file with your results to share in the project folder")
print("Thank you for running the study!")


