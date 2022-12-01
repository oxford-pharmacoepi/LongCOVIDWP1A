
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

#  IDs of the cohorts of interest ----
# Cohort ids are in a csv with all cohorts ids
cohorts_ids  <- read.csv2(here("ATLAS Cohort Definitions_LongCovid.csv")) %>%
  mutate(name= paste(sub(".*LongCov-ER_", "", name)))

get_id_interest <- function(name_id) {
  cohorts_ids %>%
    filter(name == name_id) %>% 
    select(cohort_definition_id) %>%
    pull()
}

new_infection_id            <- get_id_interest("New_Infection") 
first_infection_id          <- get_id_interest("First_infection")
reinfection_id              <- get_id_interest("Reinfection") 
tested_negative_all_id      <- get_id_interest("Tested_Negative_all_events") 
tested_negative_earliest_id <- get_id_interest("Tested negative, earliest") 
long_covid_id               <- get_id_interest("Long COVID-19 symptoms (with test start date)")
long_covid_symptom_id       <- get_id_interest("Long COVID-19 symptoms (with symptom start date)") 



# Initial Json cohorts ----
if(create_initial_json_cohorts=="FALSE"){
print(paste0("- Skipping creating initial json cohorts"))
} else { 
print(paste0("- Creating initial json cohorts"))

  # Tables in the write_schema to save Inital Json cohorts
  cohortTableJsons <-"er_cohorts_for_longcov"
  
### Get cohort details -----
 cohortJsonFiles <- list.files(here("Cohort_Dx_initial","1_InstantiateCohorts", "Cohorts"))
cohortJsonFiles <- cohortJsonFiles[str_detect(cohortJsonFiles,".json")]

cohortDefinitionSet <- list()
for(i in 1:length(cohortJsonFiles)){
working.json<-here("Cohort_Dx_initial","1_InstantiateCohorts", "Cohorts",
                      cohortJsonFiles[i])
cohortJson <- readChar(working.json, file.info(working.json)$size)
cohortExpression <- cohortExpressionFromJson(cohortJson) # generates the sql
sql <- buildCohortQuery(cohortExpression, 
                   options = CirceR::createGenerateOptions(generateStats = TRUE))

cohortDefinitionSet[[i]]<-tibble(atlasId = i,
      cohortId = i,
      cohortName = str_replace(cohortJsonFiles[i],".json",""),
      json=cohortJson,
      sql=sql,
      logicDescription = NA,
      generateStats=TRUE)
}
cohortDefinitionSet<-bind_rows(cohortDefinitionSet)

# Names of tables to be created during study run ----- 
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTableJsons)

# Create the tables in the database -----
CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                    cohortTableNames = cohortTableNames,
                                    cohortDatabaseSchema = write_schema)

# Generate the cohort set -----
CohortGenerator::generateCohortSet(connectionDetails= connectionDetails,
                                   cdmDatabaseSchema = cdm_database_schema,
                                   cohortDatabaseSchema = write_schema,
                                   cohortTableNames = cohortTableNames,
                                   cohortDefinitionSet = cohortDefinitionSet)

# Get stats  -----
CohortGenerator::exportCohortStatsTables(
    connectionDetails = connectionDetails,
    connection = NULL,
    cohortDatabaseSchema = write_schema,
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = here("Cohort_Dx_initial", "Results"),
    incremental = FALSE)

# cohort diagnostics
# executeDiagnostics(cohortDefinitionSet,
#                    connectionDetails = connectionDetails,
#                    cohortTable = cohortTableJsons,
#                    cohortDatabaseSchema = write_schema,
#                    cdmDatabaseSchema = cdm_database_schema,
#                    exportFolder = here("Cohort_Dx_initial", "Results"),
#                    databaseId = db.name,
#                    minCellCount = 5,
#                    runInclusionStatistics = FALSE,
#                    runOrphanConcepts = FALSE,
#                    runTimeDistributions = FALSE,
#                    runVisitContext = FALSE,
#                    runBreakdownIndexEvents = FALSE,
#                    runIncidenceRate = FALSE,
#                    runTimeSeries = FALSE,
#                    runCohortOverlap = FALSE,
#                    runCohortCharacterization = TRUE,
#                    runTemporalCohortCharacterization = FALSE)

# drop cohort stats table
CohortGenerator::dropCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = write_schema,
    cohortTableNames = cohortTableNames,
    connection = NULL)

rm(cohortDefinitionSet, cohortExpression,cohortTableNames)

}


# Creating Long Covid cohorts ----
if(create_long_covid_cohorts=="FALSE"){
print(paste0("- Skipping creating Long Covid cohorts"))
} else { 
print(paste0("- Creating Long Covid cohorts"))
 
# cdm object to access the tables easily
cdm <- cdm_from_con(db, 
             cdm_schema = cdm_database_schema,
             write_schema = write_schema,
             cohort_tables = c("er_cohorts_for_longcov"))

conn <- connect(connectionDetails)   

# Ids for initial filtering
covid_ids    <- cohorts_ids %>% filter(type=="covid-19") %>% select(cohort_definition_id) %>% pull()
control_id <- cohorts_ids %>% filter(name=="Tested_Negative_all_events") %>% select(cohort_definition_id) %>% pull()
# censoring cohorts
covid_censoring_id <- cohorts_ids %>% filter(type=="covid_censoring") %>% select(cohort_definition_id) %>% pull()
influenza_id <- cohorts_ids %>% filter(type=="influenza") %>% select(cohort_definition_id) %>% pull()
# symptoms cohorts
symptoms_ids <- cohorts_ids %>% filter(type=="symptom") %>% select(cohort_definition_id) %>% pull()

## get initial cohorts ----
cohorts <- cdm$er_cohorts_for_longcov %>% compute()

# combine observation period (to have observation period end date) and death with our study cohorts
observation_death <- cdm$observation_period %>%
  select(person_id, observation_period_end_date) %>%
  left_join(cdm$death %>% select (person_id, death_date)) %>%
  mutate(death = ifelse(!(is.na(death_date)), 1,0))%>%
  compute()
# covid  cohorts 
covid_cohorts <- cohorts %>%
 filter(cohort_definition_id %in% covid_ids) %>%
 left_join(observation_death, by = c("subject_id" = "person_id") )%>%
 select(-cohort_end_date) %>%
 compute()  
# control cohort 
control_cohort <- cohorts %>%
 filter(cohort_definition_id == control_id) %>%
 left_join(observation_death, by = c("subject_id" = "person_id") )%>%
 select(-cohort_end_date) %>%
 compute()
# censoring cohorts
influenza_cohort <- cohorts %>%
  filter(cohort_definition_id == influenza_id) %>%
  select(cohort_start_date, subject_id) %>%
  rename(influenza_start_date= cohort_start_date) %>%
  compute() 

covid_censoring_cohort <- cohorts %>%
    filter(cohort_definition_id==covid_censoring_id)%>%
    rename(covid_censoring_date = cohort_start_date) %>%
    select(subject_id, covid_censoring_date) %>%
    compute()
# symptom cohorts
symptom_cohorts <- cohorts %>%
 filter(cohort_definition_id %in% symptoms_ids) %>%
 select(-cohort_end_date) %>%
 rename(symptom_date = cohort_start_date) %>%
 rename(symptom_definition_id = cohort_definition_id) %>%
 select(symptom_definition_id, subject_id, symptom_date) %>%
 compute()

rm(covid_ids, influenza_id, covid_censoring_id, cohorts, observation_death)

generate_tested_cohorts <- function(cohorts_interest, covid_id, first_infection_id, reinfection_id, days_censor_covid){
  # get the cohort of interst
covid_infection <- cohorts_interest %>%
  # not needed at this point but could be escalated to have different covid cohorts
  filter(cohort_definition_id == covid_id) %>%
  group_by(subject_id) %>% 
  arrange(cohort_start_date) %>%
  # get number of infection per person
  mutate(seq=row_number()) %>% 
  distinct() %>%
  ungroup() %>%
  compute()

# Censor on new Covid infections 42 days or 0 days after first infection
censoring_covid <- covid_censoring_cohort %>%
  inner_join(covid_infection) %>%
  select(covid_censoring_date, cohort_start_date, subject_id) %>%
  filter(covid_censoring_date> cohort_start_date+ lubridate::days(days_censor_covid)) %>%
  distinct() %>%
  #  keep first covd record per person and cohort start date
  group_by(subject_id, cohort_start_date) %>%
  filter(covid_censoring_date== min(covid_censoring_date)) %>%
  ungroup() %>%
  compute()
#  Censor on influenza infections
influenza_covid <- influenza_cohort %>%
  inner_join(covid_infection) %>%
  select(influenza_start_date, cohort_start_date, subject_id) %>%
  filter(influenza_start_date>cohort_start_date) %>%
  distinct() %>%
  # keep firt influenza record per person and cohort start date
  group_by(subject_id, cohort_start_date) %>%
  filter(influenza_start_date == min(influenza_start_date)) %>%
  ungroup() %>%
  compute()

# add dates of covid and influenza for censoring
covid_infection <- covid_infection %>%
  left_join(censoring_covid) %>%
  left_join(influenza_covid) %>%
  compute()

# add date one year after infection & end of testing - country specific
covid_infection <- covid_infection %>%
mutate(one_year_date = lubridate::as_date(cohort_start_date+lubridate::days(365))) %>%
mutate(end_covid_testing_date =  as_date(covid_end_date)) %>%
compute()

# add cohort end date, considering censoring options
# death, observation end, next covid infection, influenza, one year followup
covid_infection  <- covid_infection %>% 
mutate(cohort_end_date = lubridate::as_date(pmin(
                                observation_period_end_date, 
                                death_date, 
                                covid_censoring_date-lubridate::days(1), 
                                influenza_start_date-lubridate::days(1), 
                                one_year_date,
                                end_covid_testing_date,
                                na.rm = F))) %>%
mutate(follow_up_days = cohort_end_date - cohort_start_date) %>% 
mutate(reason_censoring = ifelse(!(is.na(covid_censoring_date)) & cohort_end_date == covid_censoring_date-lubridate::days(1), 
                                 "COVID-19", 
                          ifelse(!(is.na(influenza_start_date)) & cohort_end_date == influenza_start_date-lubridate::days(1),
                                 "influenza",
                          ifelse(!(is.na(death_date)) & cohort_end_date == death_date, 
                                  "death",
                          ifelse(cohort_end_date == one_year_date,
                                   "one year of follow_up", 
                          ifelse(cohort_end_date == end_covid_testing_date,
                                 "End of COVID-19 testing",
                          ifelse(cohort_end_date == observation_period_end_date,
                                   "end of data collection or exit from database",
                                                      NA ))))))) %>%
  compute()

covid_infection <- covid_infection %>% 
  select(cohort_definition_id, subject_id, cohort_start_date, cohort_end_date, seq, follow_up_days, reason_censoring) %>%
  compute()

# pop
exclusion_table <- tibble(N_current=covid_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="initial pop",
                          cohort_definition_id = covid_id)
# keep only infections after covid start date - defined in Code to Run
covid_infection <- covid_infection %>%
 filter(cohort_start_date>=covid_start_date) %>%
 compute()

exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=covid_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason=paste0("Tests prior to ", covid_start_date),
                          cohort_definition_id = covid_id
                         ))
# summary(covid_infection %>% select(follow_up_days) %>% distinct() %>%collect()) 

# keep only records with at least 120 days of follow-up
excluded_less_120 <- covid_infection %>%
  filter(follow_up_days <120) %>%
  compute()

reason_exclusion <-  excluded_less_120 %>%
group_by(reason_censoring) %>%
  tally()%>%
  collect()
 
covid_infection <- covid_infection %>%
  filter(!(follow_up_days<120))%>%
  select(-follow_up_days, -reason_censoring) %>%
  compute()

#check follow-up days are fine

exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=covid_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="Less than 120 days of follow-up",
                          cohort_definition_id = covid_infection %>%
                                                 select(cohort_definition_id) %>% 
                            distinct() %>%pull()
                         ))

### First COVID-19 infection  
first_infection <- covid_infection   %>%
  filter(seq==1) %>% 
  select(-seq) %>%
  # we change cohort definition id 
  mutate(cohort_definition_id=as.integer(first_infection_id)) %>%
  compute() 

# check we have one row per person
# first_infection %>%tally()    # 271840
# first_infection %>% select(subject_id)%>% distinct() %>%tally() #271840

# we add the cohort id to our cohorts ids table

exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=first_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="Population included",
                          cohort_definition_id = first_infection_id
                         ))

### Reinfections
reinfection <- covid_infection  %>%
  filter(seq!=1)%>% 
  select(-seq) %>%
  # we change cohort definition id so that is has its own id
  mutate(cohort_definition_id=as.integer(reinfection_id)) %>%
  compute()


exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=reinfection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="Population included",
                          cohort_definition_id = reinfection_id
                          ))
# get rid of seq in covid infection
covid_infection <- covid_infection %>%
   select(-seq) %>%
   mutate(cohort_definition_id= as.integer(cohort_definition_id))%>%
   compute()

result <- list(covid_infection, first_infection, reinfection, exclusion_table, reason_exclusion) 
result
} 

## getting COVID-19 cohorts ----
confirmed_infections <- generate_tested_cohorts(cohorts_interest = covid_cohorts,
                                                covid_id = new_infection_id,
                                                         first_infection_id = first_infection_id,
                                                         reinfection_id = reinfection_id,
                                                         days_censor_covid = 42
                                                         )
covid_infection <- confirmed_infections[[1]]
first_infection <- confirmed_infections[[2]]
reinfection     <- confirmed_infections[[3]]
## inserting these cohorts into the database
#### remove prior tables if needed
  sql_query <- glue::glue("DROP TABLE {write_schema}.er_long_covid_final_cohorts\n" )
  DBI::dbExecute(db, as.character(sql_query))

 # create new table 
sql_query <- glue::glue("SELECT * INTO {write_schema}.er_long_covid_final_cohorts\n",
                          "FROM (\n",
                          dbplyr::sql_render(covid_infection),
                          "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))

# function to append rows to existing table - using sql
add_sql_query <- function(tbl_df){
sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
                          "SELECT * FROM (\n",
                          dbplyr::sql_render(tbl_df),
                          "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query)) 
}



add_sql_query(first_infection)
add_sql_query(reinfection)

## getting Tested negative cohorts ----
### PCR and antigen negative _all events 
# id of interest
tested_negative_re <- 1250 # we don't care repeated tests negatives - we would not use that
negative_infections <- generate_tested_cohorts(cohorts_interest = control_cohort,
                                                covid_id = tested_negative_all_id,
                                                first_infection_id = tested_negative_earliest_id,
                                                reinfection_id = tested_negative_re,
                                                days_censor_covid = 0)


tested_negative_all      <- negative_infections[[1]]
tested_negative_earliest <- negative_infections[[2]]

# add tested negative cohorts to db
add_sql_query(tested_negative_all)
add_sql_query(tested_negative_earliest)

#get exclusion table and save
exclusion_table <- rbind(confirmed_infections[[4]], negative_infections[[4]])
excluded_shorter_follow_up <- rbind(confirmed_infections[[5]] %>%mutate(cohort= "COVID-19"),
                                    negative_infections[[5]]%>%mutate(cohort= "Negative"))

write.csv2(exclusion_table, here(paste0("results/exclusion_table_", database_name, ".csv")), row.names = FALSE)
write.csv2(excluded_shorter_follow_up, here(paste0("results/exclusions_followup_", database_name, ".csv")), row.names = FALSE)

## getting Symptoms cohorts  ----
### parameters for loop
symptoms_ids   <- cohorts_ids %>% filter(type=="symptom") %>% select(cohort_definition_id) %>% pull()
window_longCov <- c(28,90)

cohorts_list    <- list()
excluded_list   <- list()
denominators_df <- tibble()
## parameters  For testing the loop/function
 #symptoms_ids <- c(1,2,3)  # for testing just 3 symptoms
#    i <- 2 # for testing - testing with only one symptom id
#    j <- 1 # for testing - washout window
# cohort <- reinfections

## function including loop to generate all cohorts  
creating_symptom_cohorts <- function(cohort){
for (i in 1:length(symptoms_ids)){
 message(paste0("working on ", symptoms_ids[i]))
  output <- tibble()
  excluded <- tibble()

  symptom_id <-symptoms_ids[i]
  # we get our symptom of interest cohort
  symptom <- symptom_cohorts %>%
             filter(symptom_definition_id==symptom_id) %>%
             compute()
  # we run everything using two different washout windows
  for (j in 1:length(window_longCov)){
     message(paste0("working on ", window_longCov[j]))
  window <- window_longCov[j]
  # we first clean symptoms considering covid infection date
  symptom_covid <- symptom %>%
  inner_join(cohort)%>%
    # we keep only symptoms recorded between 180 days prior to the index date and the cohort end date
  filter(!(symptom_date<cohort_start_date-lubridate::days(180))) %>%  
  filter(symptom_date<=cohort_end_date) %>%
  mutate(days_symptom_onset= symptom_date - cohort_start_date) %>% 
  distinct() %>%
  compute()
  # we find the people we need to exclude (symptom within the washout window) -
  # it's people with a negative number for days since symptom onset
  people_to_exclude <- symptom_covid %>%
  filter(days_symptom_onset<=0) %>%
  select(subject_id, cohort_start_date) %>%
  compute()
## exclude considering both id & cohort_start_date (someone with more than one infection can be included for one infection but not the other)
symptom_covid <-symptom_covid %>%
  anti_join(people_to_exclude) %>%
  # exclude rows with symptoms recorded between index date and window (90 or 28 days)
  filter(!(symptom_date< cohort_start_date+lubridate::days(window))) %>%
  distinct() %>%
  compute()
# since people can have more than one symptom recorded, we just keep the first one
symptom_covid <- symptom_covid %>%
  group_by(subject_id, cohort_start_date) %>%
  arrange(symptom_date) %>%
  mutate(seq2= row_number()) %>%
  ungroup() %>%
  compute()
symptom_covid <- symptom_covid %>%
                 filter(seq2==1) %>%
                 select(-seq2) %>%
                 compute()
  
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
  compute()

working_symptom <- working_symptom %>%
  mutate(year = lubridate::year(cohort_start_date),
         month = lubridate::month(cohort_start_date)) %>%
  mutate(trimester=
                   ifelse(year==2020 & month>8, "Sept_Dec_2020",
                   ifelse(year==2021 & month<4, "Jan_Mar_2021",
                   ifelse(year==2021 & (month==4 | month==5| month==6), "Apr_Jun_2021",
                   ifelse(year==2021 & (month==7 | month==8| month==9), "Jul_Sep_2021",
                   ifelse(year==2021 & month>9, "Oct_Dec_21", 
                   NA)))))) %>%
  select(-c(year,month)) %>%
  compute()


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

working_denominator <- working_denominator %>%
  cbind(working_den_trimester)

denominators_df <- rbind(denominators_df, working_denominator)

# now we keep only people with the symptom- to generate the cohorts
working_symptom <- working_symptom %>%
  filter(!(is.na(symptom_date))) %>%
  select(cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) %>%
  compute()
# insert cohort into the database
add_sql_query(working_symptom)
# collect to use later
# working_symptom <- working_symptom %>%
# collect
# we also save this into a list to generate the any symptom cohort
output <- rbind(output,working_symptom)

people_to_exclude <- people_to_exclude %>%
                     collect() %>%
                      mutate(cohort_definition_id = working_symptom %>% 
                               select(cohort_definition_id)%>%
                               distinct() %>% 
                               pull())

excluded <- rbind(excluded, people_to_exclude)

  }
cohorts_list[[i]] <- output
excluded_list[[i]] <- excluded
denominators_df
}
result <- list(cohorts_list, denominators_df, excluded_list)  
}

# Covid cohorts & control cohorts 
covid_infection_symp          <- creating_symptom_cohorts(cohort = covid_infection)
tested_negative_all_symp      <- creating_symptom_cohorts(cohort = tested_negative_all)
# first_infection_symp          <- creating_symptom_cohorts(cohort = first_infection)
# reinfections_symp             <- creating_symptom_cohorts(cohort = reinfection)
# tested_negative_earliest_symp <- creating_symptom_cohorts(cohort = tested_negative_earliest )


denominator <- rbind(covid_infection_symp[[2]],
                    # first_infection_symp[[2]],
                    # reinfections_symp[[2]] ,
                    # tested_negative_earliest_symp[[2]],
                     tested_negative_all_symp[[2]])

# save  denominator table
write.csv2(denominator, here(paste0("results/denominator_each_symptom_", database_name, ".csv")),row.names = FALSE)

#collect cohorts if R session aborted - it always happens with CPRD
cdm <- cdm_from_con(db, 
                    cdm_schema = cdm_database_schema,
                    write_schema = write_schema,
                    cohort_tables = c("er_long_covid_final_cohorts")) # here are the cohorts with all symptoms

ids_to_collect <- cdm$er_long_covid_final_cohorts %>% select(cohort_definition_id) %>% distinct() %>% arrange(cohort_definition_id) %>%
                 collect() %>% mutate(main_cohort_id = substr(as.character(cohort_definition_id), 1,2)) %>%
  filter(!(cohort_definition_id %in% c(22,28,70,71,75,229928,229990,289928,289990)))

tested_positive_ids <- ids_to_collect %>% filter(main_cohort_id== new_infection_id) %>% select(cohort_definition_id) %>%pull()
tested_negative_ids <- ids_to_collect %>% filter(main_cohort_id== tested_negative_all_id) %>% select(cohort_definition_id) %>%pull()

cohorts_list <- list()
collect_cohorts <- function(ids){
for (i in 1:length(ids)){
  message(paste0("working on ", ids[i]))
    # collect to use later
  id <- ids[i]
  output <- tibble()
  cohort <- cdm$er_long_covid_final_cohorts %>% 
    filter(cohort_definition_id == id) %>%
    collect()
  
  output <- rbind(output,cohort)
  cohorts_list[[i]] <- output
  }

cohorts_list
}

covid_infection_symp     <- collect_cohorts(ids = tested_positive_ids)
tested_negative_all_symp <-  collect_cohorts(ids = tested_negative_ids)

# generate any symptom cohorts 

## function to generate any sypmtom cohort
create_any_symptom <- function(list_cohorts){
  # we first bind the rows from the previous function result
  any_symp_cohort <- bind_rows(list_cohorts) %>%
    # we generate a new cohort_definition_id -- for any symtpom we use 99
    mutate(cohort_definition_id = 
             (as.numeric(substr(cohort_definition_id, 1, 2))*10^4) +
             as.numeric(long_covid_id)*10^2+
             as.numeric(substr(cohort_definition_id, 
                               nchar(cohort_definition_id[1])-1, 
                               nchar(cohort_definition_id[1])))) %>%
    distinct()  %>%
    group_by(subject_id, cohort_definition_id, cohort_start_date) %>%
    arrange(cohort_end_date) %>% 
    # we keep just first symptom recorded by unique cohort, subject id and cohort start id
    mutate(seq=row_number()) %>%
    filter(seq==1) %>%
    ungroup() %>%
    select(-seq)
  
}


covid_infection_any_symp          <- create_any_symptom(list_cohorts = covid_infection_symp)
tested_negative_all_any_symp      <- create_any_symptom(list_cohorts = tested_negative_all_symp)
# first_infection_any_symp          <- create_any_symptom(list_cohorts = first_infection_symp)
# reinfections_any_symp             <- create_any_symptom(list_cohorts = reinfections_symp)
# tested_negative_earliest_any_symp <- create_any_symptom(list_cohorts = tested_negative_earliest_symp )

# to remove a specific cohort if needed
# dbExecute(db, "DELETE FROM results.er_long_covid_final_cohorts WHERE cohort_definition_id = 289928")


# append any symptoms cohorts to the table
append_table<- function(data){
  insertTable(
    connection = conn ,
    tableName = paste0(write_schema, ".", "er_long_covid_final_cohorts"),
    data =data,
    dropTableIfExists = FALSE,
    createTable = FALSE
  )
}  
append_table(covid_infection_any_symp)
append_table(tested_negative_all_any_symp)
# append_table(first_infection_any_symp)
# append_table(reinfections_any_symp)
# append_table(tested_negative_earliest_any_symp)

# generate cohorts inclding all symptoms per person&infection 
 
# cohort including all the symptoms reported per row
create_numb_symptom <- function(list_cohorts){
symps_cohort <- bind_rows(list_cohorts)%>%
  mutate(main_cohort_id = substr(as.character(cohort_definition_id), 1,2)) %>%
  mutate(symptom_id = substr(as.character(cohort_definition_id), 3, 4)) %>%
  mutate(window_days = substr(as.character(cohort_definition_id), 5, 6)) %>%
  mutate(symptom_id = as.numeric(symptom_id)) %>%
  mutate(symptom =1) %>%  
  left_join(cohorts_ids %>% 
              select(cohort_definition_id, name_var) %>% 
              rename(symptom_id=cohort_definition_id)) %>%
  select(-cohort_end_date, -symptom_id, -cohort_definition_id) %>%
  mutate(name_var = str_to_lower(name_var)) %>%
  group_by(subject_id, cohort_start_date, window_days) %>%
  spread(name_var, symptom) %>%
    ungroup()  %>%
  #create variable for each symptom if it doesn't exist
  mutate(abdominal_pain = if (exists('abdominal_pain', where = .)) abdominal_pain else 0) %>%
  mutate(allergy = if (exists('allergy', where = .)) allergy else 0) %>%
  mutate(altered_smell_taste = if (exists('altered_smell_taste', where = .)) altered_smell_taste else 0) %>%
  mutate(anxiety = if (exists('anxiety', where = .)) anxiety else 0) %>%
  mutate(blurred_vision = if (exists('blurred_vision', where = .)) blurred_vision else 0) %>%
  mutate(chest_pain = if (exists('chest_pain', where = .)) chest_pain else 0) %>%
  mutate(cough = if (exists('cough', where = .)) cough else 0) %>%
  mutate(cognitive_dysfunction_brain_fog=ifelse(is.na(cognitive_dysfunction_brain_fog),0,cognitive_dysfunction_brain_fog)) %>% 
  mutate(depression = if (exists('depression', where = .)) depression else 0) %>%
  mutate(dizziness = if (exists('dizziness', where = .)) dizziness else 0) %>%
  mutate(dyspnea = if (exists('dyspnea', where = .)) dyspnea else 0) %>%
  mutate(fatigue_malaise = if (exists('fatigue_malaise', where = .)) fatigue_malaise else 0) %>%
  mutate(gastrointestinal_issues = if (exists('gastrointestinal_issues', where = .)) gastrointestinal_issues else 0) %>%
  mutate(headache = if (exists('headache', where = .)) headache else 0) %>%
  mutate(intermittent_fever = if (exists('intermittent_fever', where = .)) intermittent_fever else 0) %>%
  mutate(joint_pain = if (exists('joint_pain', where = .)) joint_pain  else 0) %>%
  mutate(memory_issues = if (exists('memory_issues', where = .)) memory_issues else 0) %>%
  mutate(menstrual_problems = if (exists('menstrual_problems', where = .)) menstrual_problems  else 0) %>%
  mutate(muscle_spams_pain = if (exists('muscle_spams_pain', where = .)) muscle_spams_pain else 0) %>%
  mutate(neuralgia = if (exists('neuralgia', where = .)) neuralgia else 0) %>%
  mutate(pins_sensation = if (exists('pins_sensation', where = .))  pins_sensation else 0) %>%
  mutate(postexertional_fatigue = if (exists('postexertional_fatigue', where = .))postexertional_fatigue  else 0) %>%
  mutate(sleep_disorder = if (exists('sleep_disorder', where = .)) sleep_disorder  else 0) %>%
  mutate(tachycardia = if (exists('tachycardia', where = .)) tachycardia else 0) %>%
  mutate(tinnitus_and_hearing_problems = if (exists('tinnitus_and_hearing_problems', where = .)) tinnitus_and_hearing_problems else 0) 

symps_cohort$all_symp<-rowSums(symps_cohort[,5:29],na.rm=T)
symps_cohort 
}

covid_infection_numb_symp          <- create_numb_symptom(list_cohorts = covid_infection_symp)
tested_negative_all_numb_symp      <- create_numb_symptom(list_cohorts = tested_negative_all_symp)
# first_infection_numb_symp          <- create_numb_symptom(list_cohorts = first_infection_symp)
# reinfections_numb_symp             <- create_numb_symptom(list_cohorts = reinfections_symp)
# tested_negative_earliest_numb_symp <- create_numb_symptom(list_cohorts = tested_negative_earliest_symp )


# sql_query <- glue::glue("DROP TABLE {write_schema}.er_long_covid_all_symptoms_cohorts\n" )
# DBI::dbExecute(db, as.character(sql_query))
# save this in a different table in the database, since it has a different structure
# create the new table
  insertTable(
  connection = conn ,
  tableName = paste0(write_schema, ".", "er_long_covid_all_symptoms_cohorts"),
  data =covid_infection_numb_symp,
  dropTableIfExists = TRUE,
  createTable = TRUE
)
# append the others  
append_table_all<- function(data){
  insertTable(
  connection = conn ,
  tableName = paste0(write_schema, ".", "er_long_covid_all_symptoms_cohorts"),
  data =data,
  dropTableIfExists = FALSE,
  createTable = FALSE
)
}  
append_table_all(tested_negative_all_numb_symp)
# append_table_all(first_infection_numb_symp)
# append_table_all(reinfections_numb_symp)
# append_table_all(tested_negative_earliest_numb_symp)

# dbExecute(db, "DELETE FROM results.er_long_covid_final_cohorts WHERE cohort_definition_id = 288928")
# dbExecute(db, "DELETE FROM results.er_long_covid_final_cohorts WHERE cohort_definition_id = 228928")
# save the cohort for any symptoms keeping the date of start of symptoms as cohort_start_date 
# to use these cohorts with the IncidencePrevalence package
# these cohorts need a different id - 
change_any_symptom_date <- function(data){
  data <- data %>%
  mutate(cohort_start_date = cohort_end_date) %>%
  mutate(cohort_definition_id = (as.numeric(
                         substr(cohort_definition_id, 1, 2))*10^4 +
                         as.numeric(long_covid_symptom_id)*10^2+
                         as.numeric(substr(cohort_definition_id, 5,6)
                                    ))) %>%
  compute()
  data
}

covid_infection_any_symp          <- change_any_symptom_date(covid_infection_any_symp)
tested_negative_all_any_symp      <- change_any_symptom_date(tested_negative_all_any_symp)
# first_infection_any_symp          <- change_any_symptom_date(first_infection_any_symp)
# reinfections_any_symp             <- change_any_symptom_date(reinfections_any_symp)
# tested_negative_earliest_any_symp <- change_any_symptom_date(tested_negative_earliest_any_symp)

append_table(covid_infection_any_symp)
append_table(tested_negative_all_any_symp)
# append_table(first_infection_any_symp)
# append_table(reinfections_any_symp)
# append_table(tested_negative_earliest_any_symp)


# exclusion table


}

# Generate data for Descriptive Tables ----
if(generate_data_descriptive_table=="FALSE"){
print(paste0("- Skipping generating data for descriptive tables"))
} else { 
print(paste0("- Creating data for descriptive tables"))
  
# Generate new cdm object to access new tables
cdm <- cdm_from_con(db, 
             cdm_schema = cdm_database_schema,
             write_schema = write_schema,
             cohort_tables = c("er_cohorts_for_longcov", # here are the vaccines talbes 
                               "er_long_covid_final_cohorts",  # here are the long covid cohorts
                               "er_long_covid_all_symptoms_cohorts")) # here are the cohorts with all symptoms

# get vaccines and PCR ids 
get_id_interest_detect <- function(string) {
  cohorts_ids %>%
    filter(str_detect(name, string)) %>% 
    select(cohort_definition_id) %>%
    pull()
}

astrazeneca_id <- get_id_interest_detect("AstraZeneca")
janssen_id     <- get_id_interest_detect("Janssen")
pfizer_id      <- get_id_interest_detect("Pfizer")
moderna_id     <- get_id_interest_detect("Moderna")
pcr_id         <- get_id_interest_detect("PCR_test_description")

pcr_test <- cdm$er_cohorts_for_longcov %>%
  filter(cohort_definition_id == pcr_id) %>%
  select(subject_id, cohort_start_date) %>%
   mutate(pcr = 1 ) %>%
   rename(person_id = subject_id) %>%
  compute()

# function to get baseline characteristics in each cohort
#! fix vaccines when needed----
get_baseline_characteristics <- function(main_cohort_id){
                                             
working_cohort <-   cdm$er_long_covid_final_cohorts %>% 
  filter(cohort_definition_id == main_cohort_id) %>%
  rename(person_id=subject_id) %>%
  left_join(cdm$person  %>%
              select(person_id, gender_concept_id, year_of_birth) %>%
              mutate(gender= ifelse(gender_concept_id==8507, "Male",
                             ifelse(gender_concept_id==8532, "Female", NA ))) %>%
              select(-gender_concept_id)) %>%
  # SIDIAP specific  - MEDEA 
 # left_join(cdm$observation %>%
 #             filter(observation_source_value == "medea") %>% 
 #             select(person_id, observation_source_value, value_as_string) %>%
 #             filter(value_as_string %in% c("R", "U1", "U2", "U3", "U4", "U5")) %>%
 #             mutate(medea=value_as_string) %>%
 #             select(person_id, medea)) %>%
  compute()
         
  # get age 
    working_cohort<- working_cohort %>%
    mutate(age= year(cohort_start_date)-year_of_birth) %>%
    select(-year_of_birth) %>%
      mutate(age_gr2=ifelse(age<=34,         "<=34",
                 ifelse(age>=35 & age<=49,  "35-49",    
                 ifelse(age>=50 & age<=64,  "50-64",    
                 ifelse(age>=65 & age<=79,  "65-79",    
                 ifelse(age>=80,             ">=80",
                       NA)))))) %>% 
      # 5y age band for matching
   mutate(age_gr5=ifelse(age<20,  "<20",
                  ifelse(age>=20 & age<=24,  "20-24", 
                  ifelse(age>=25 & age<=29,  "25-29",    
                  ifelse(age>=30 & age<=34,  "30-34",    
                  ifelse(age>=35 & age<=39,  "35-39",   
                  ifelse(age>=40 & age<=44,  "40-44",    
                  ifelse(age>=45 & age<=49,  "45-49",    
                  ifelse(age>=50 & age<=54,  "50-54",               
                  ifelse(age>=55 & age<=59,  "55-59",  
                  ifelse(age>=60 & age<=64,  "60-64",    
                  ifelse(age>=65 & age<=69,  "65-69",
                  ifelse(age>=70 & age<=74,  "70-74",
                  ifelse(age>=75 & age<=79,  "75-79",
                  ifelse(age>=80 & age<=84,  "80-84",
                  ifelse(age>=85 & age<=89,  "85-89",
                  ifelse(age>=90 & age<=94,  "90-94",
                  ifelse(age>=95 & age<=99,  "95-99",
                  ifelse(age>=100 , ">=100",
                       NA))))))))))))))))))) %>%
      # get week and year of test for matching
  compute() 


 # add pcr tests, yes or no 
 working_cohort <- working_cohort %>%
   left_join(pcr_test) %>%
   compute()
 
 # add vaccines 
  vaccines <- union_all(cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == astrazeneca_id) %>% mutate(vaccine_type = "ChAdOx1"),
                  cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == moderna_id) %>% mutate(vaccine_type = "mRNA-1273"),
                  cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == pfizer_id)  %>% mutate(vaccine_type = "BNT162b2"),
                  cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == janssen_id) %>% mutate(vaccine_type = "Ad26.COV2.S")
                  ) %>%
   rename(drug_exposure_start_date = cohort_start_date,
          person_id = subject_id) %>%
    select(-cohort_end_date, -cohort_definition_id) %>%
    full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
    filter(drug_exposure_start_date < cohort_start_date) %>%
    distinct() %>%
    compute()
 
# get  dates and vaccines types for the different doses
vaccines <- vaccines %>%
      group_by(person_id) %>% 
      arrange(drug_exposure_start_date) %>% 
      mutate(seq=row_number())   %>%
      mutate(second_dose_type = ifelse(seq==1, lead(vaccine_type, order_by = c("drug_exposure_start_date")),  NA),
             second_dose_date = ifelse(seq==1, lead(drug_exposure_start_date, order_by = c("drug_exposure_start_date")), NA)) %>%
  ungroup() %>%
  compute()

vaccines <- vaccines %>%
  filter(seq!=2) %>%
  group_by(person_id) %>%
  arrange(drug_exposure_start_date) %>% 
  mutate( third_dose_type = ifelse(seq==1,lead(vaccine_type, order_by = c("drug_exposure_start_date")), NA),
             third_dose_date = ifelse(seq==1, lead(drug_exposure_start_date, order_by = c("drug_exposure_start_date")), NA)) %>%
  ungroup() %>%
  filter(seq==1) %>%
  rename(first_dose_type = vaccine_type,
         first_dose_date = drug_exposure_start_date) %>%
  select( -seq) %>%
  mutate(one_dose_vaccine = ifelse(is.na(first_dose_date), 0, 1)) %>%
  mutate(fully_vaccinated = ifelse(first_dose_type=="Ad26.COV2.S", 1,
                                   ifelse(is.na(second_dose_date), 0,1))) %>%
  mutate(booster_doses = ifelse(first_dose_type=="Ad26.COV2.S" & !(is.na(second_dose_date)),  1,
                                ifelse(is.na(third_dose_date), 0,1))) %>%
  mutate(vaccination_status = ifelse(booster_doses==1, "Booster doses", 
                              ifelse( fully_vaccinated ==1, "Two doses vaccination",
                              ifelse( one_dose_vaccine == 1, "First dose vaccination", NA)
                              ))) %>%
  compute()

 
working_cohort <- working_cohort%>%
  left_join(vaccines)%>% 
  mutate(vaccination_status = ifelse(is.na(vaccination_status), "Non vaccinated", vaccination_status)) %>%
  compute()
 
 #### Comorbidities at baseline
autoimmune_disease.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==434621) %>% 
   collect()
 
 autoimmune_disease <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!autoimmune_disease.codes$descendant_concept_id) %>% 
   full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
 
 asthma.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==317009) %>% 
   collect()
 
 asthma <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!asthma.codes$descendant_concept_id) %>% 
   full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
 
 malignant_neoplastic_disease.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==443392) %>% 
   collect()
 
 malignant_neoplastic_disease <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!malignant_neoplastic_disease.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
     compute() 
  
 diabetes.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==201820) %>% 
   collect()
 
 diabetes <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!diabetes.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
      compute() 
 
 heart_disease.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==321588) %>% 
   collect()
 
 heart_disease <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!heart_disease.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
 
 hypertensive_disorder.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==316866) %>% 
      collect() 
 
 hypertensive_disorder <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!hypertensive_disorder.codes$descendant_concept_id) %>%
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
  
 renal_impairment.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==4030518) %>% 
   collect()
 
 renal_impairment <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!renal_impairment.codes$descendant_concept_id) %>%
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
 
 copd.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==255573) %>% 
   collect()
 
 copd <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!copd.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute()
 
 dementia.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==4182210) %>% 
   collect()
 
 dementia <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!dementia.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute()
 # add to cohort
 working_cohort <- working_cohort %>%
   left_join(asthma %>% mutate(asthma = 1)) %>%
   left_join(autoimmune_disease %>% mutate(autoimmune_disease = 1)) %>%
   left_join(copd %>% mutate(copd = 1 )) %>%
   left_join(dementia %>% mutate(dementia = 1)) %>%
   left_join(diabetes %>% mutate(diabetes = 1)) %>%
   left_join(heart_disease %>% mutate(heart_disease = 1)) %>%
   left_join(hypertensive_disorder %>% mutate(hypertension = 1)) %>%
   left_join(malignant_neoplastic_disease %>% mutate(cancer = 1)) %>%
   left_join(renal_impairment %>% mutate(renal_impairment = 1)) %>%
   mutate(asthma=ifelse(is.na(asthma),0,asthma)) %>% 
   mutate(autoimmune_disease=ifelse(is.na(autoimmune_disease),0,autoimmune_disease)) %>% 
   mutate(copd=ifelse(is.na(copd),0,copd)) %>% 
   mutate(dementia=ifelse(is.na(dementia),0,dementia)) %>% 
   mutate(diabetes=ifelse(is.na(diabetes),0,diabetes)) %>% 
   mutate(heart_disease=ifelse(is.na(heart_disease),0,heart_disease)) %>% 
   mutate(hypertension=ifelse(is.na(hypertension),0,hypertension)) %>% 
   mutate(cancer=ifelse(is.na(cancer),0,cancer)) %>% 
   mutate(renal_impairment=ifelse(is.na(renal_impairment),0,renal_impairment)) 
 

# add variables for timester of infection   
  working_cohort <- working_cohort %>%
  mutate(year  = lubridate::year(cohort_start_date),
         month = lubridate::month(cohort_start_date),
         week  = lubridate::week(cohort_start_date)) %>%
  mutate(trimester=
                   ifelse(year==2020 & month>8,                                    "Sep-Dec 2020",
                   ifelse(year==2021 & month<5,                                    "Jan-Apr 2021",
                   ifelse(year==2021 & (month==5 | month==6| month==7 | month==8), "May-Aug 2021",
                   ifelse(year==2021 & month>8,                                    "Sep-Dec 2021",
                   ifelse(year==2022 & month<5,                                    "Jan-Apr 2022", 
                   ifelse(year==2022 & month>=5,                                   "May 2022 or later", 
                   NA))))))) %>%
  select(-month) %>%
    compute()
 
working_cohort
}

#  get baseline characteristics and save just in case the server crashes
baseline_new_infections            <- get_baseline_characteristics(main_cohort_id = new_infection_id) 
# save just in case... 
sql_query <- glue::glue("SELECT * INTO {write_schema}.er_long_covid_baseline_new_infections\n",
                        "FROM (\n",
                        dbplyr::sql_render(baseline_new_infections),
                        "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))

baseline_tested_negative_all  <- get_baseline_characteristics(main_cohort_id = tested_negative_all_id )
sql_query <- glue::glue("SELECT * INTO {write_schema}.er_long_covid_baseline_tested_negative_all\n",
                        "FROM (\n",
                        dbplyr::sql_render(baseline_tested_negative_all),
                        "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))
# function to add long covid + symptoms 
# In long COVID cohorts, first two digits are for the "mother cohort"
# digits 3,4 are for the symptoms cohort (99 is for any symptom - so lOng covid) 
# last two digits for window period

 get_long_cov_symptoms <- function(main_cohort, main_cohort_id,  window_id){
   # identify long covid cohort specific to the main cohort and for the time window of interest
  long_covid_specific_id <- main_cohort_id*10^4+long_covid_id*10^2+window_id
  long_covid_cohort <- cdm$er_long_covid_final_cohorts %>%  filter(cohort_definition_id == long_covid_specific_id) %>% compute()
  # add this to main cohort
  working_cohort <- main_cohort %>%
    mutate(sex = ifelse(gender=="Female", 1,0)) %>%
       left_join(long_covid_cohort   %>% 
                distinct() %>%
                select(-cohort_definition_id) %>%
                mutate(long_covid = 1) %>%
                rename(long_covid_date = cohort_end_date,
                      person_id = subject_id))  %>%
    mutate(long_covid=ifelse(is.na(long_covid),0,long_covid)) %>%
compute()
 
  
 # we add long covid symmptoms  # these cohorts are saved in a different table
working_cohort <- working_cohort %>%
left_join(cdm$er_long_covid_all_symptoms_cohorts %>%
            rename(cohort_id = main_cohort_id) %>%
            mutate(cohort_id = as.numeric(cohort_id)) %>%
   filter(cohort_id == main_cohort_id) %>%
   mutate(window_days = as.numeric (window_days)) %>%
   filter(window_days == window_id) %>%
   select(-cohort_id ) %>%
   rename(person_id = subject_id)) %>% 
  mutate(window_days = ifelse(is.na(window_days), window_id, window_days)) %>%
# # replace NA for each symptom variable with 0 
  mutate(abdominal_pain= ifelse(is.na(abdominal_pain),0,abdominal_pain)) %>% 
  mutate(allergy = ifelse(is.na(allergy ),0,allergy)) %>% 
  mutate(altered_smell_taste =ifelse(is.na(altered_smell_taste ),0,altered_smell_taste)) %>% 
  mutate(blurred_vision  =ifelse(is.na(blurred_vision  ),0,blurred_vision )) %>% 
  mutate(anxiety =ifelse(is.na(  anxiety),0, anxiety)) %>% 
  mutate(chest_pain =ifelse(is.na(chest_pain ),0,chest_pain)) %>% 
  mutate(cognitive_dysfunction_brain_fog=ifelse(is.na(cognitive_dysfunction_brain_fog),0,cognitive_dysfunction_brain_fog)) %>% 
  mutate(cough  =ifelse(is.na( cough  ),0, cough )) %>% 
  mutate(depression =ifelse(is.na(depression ),0,depression)) %>% 
  mutate(dizziness =ifelse(is.na(dizziness ),0,dizziness)) %>% 
  mutate(dyspnea =ifelse(is.na( dyspnea),0,dyspnea)) %>% 
  mutate(fatigue_malaise =ifelse(is.na(fatigue_malaise ),0,fatigue_malaise)) %>% 
  mutate(gastrointestinal_issues =ifelse(is.na(gastrointestinal_issues ),0,gastrointestinal_issues)) %>% 
  mutate(headache  =ifelse(is.na( headache ),0,headache )) %>% 
  mutate(intermittent_fever =ifelse(is.na(intermittent_fever ),0,intermittent_fever)) %>% 
  mutate(joint_pain =ifelse(is.na( joint_pain ),0, joint_pain)) %>% 
  mutate(memory_issues  =ifelse(is.na(memory_issues ),0, memory_issues )) %>% 
  mutate(menstrual_problems =ifelse(is.na(menstrual_problems),0,menstrual_problems)) %>% 
  mutate(muscle_spams_pain =ifelse(is.na(muscle_spams_pain),0, muscle_spams_pain)) %>% 
  mutate(neuralgia =ifelse(is.na(neuralgia),0,neuralgia)) %>% 
  mutate(pins_sensation =ifelse(is.na(pins_sensation ),0,pins_sensation)) %>% 
  mutate(postexertional_fatigue =ifelse(is.na(postexertional_fatigue ),0,postexertional_fatigue)) %>% 
  mutate(sleep_disorder =ifelse(is.na(sleep_disorder),0, sleep_disorder)) %>% 
  mutate(tachycardia =ifelse(is.na(tachycardia ),0,tachycardia)) %>% 
  mutate(tinnitus_and_hearing_problems =ifelse(is.na(tinnitus_and_hearing_problems ),0,tinnitus_and_hearing_problems)) %>%
  mutate(all_symp = ifelse(is.na(all_symp),0,all_symp)) %>% 
  compute()
 
working_cohort
 }
  
cdm <- cdm_from_con(db, 
                    cdm_schema = cdm_database_schema,
                    write_schema = write_schema,
                    cohort_tables = c("er_cohorts_for_longcov", # here are the vaccines talbes 
                                      "er_long_covid_final_cohorts",  # here are the long covid cohorts
                                      "er_long_covid_all_symptoms_cohorts", # here are the cohorts with all symptoms
                                      "er_long_covid_baseline_new_infections", # here are baseline characteristics for tested positive
                                      "er_long_covid_baseline_tested_negative_all"
                                      ))  # here are baseline characteristics for tested negative

# add long covid symptoms , 90 days
table1_data_new_infections         <- get_long_cov_symptoms(main_cohort = cdm$er_long_covid_baseline_new_infections,
                                                            main_cohort_id = new_infection_id,
                                                            window_id  = 90)

# save results in new table in the df
# drop previous table if needed
# sql_query <- glue::glue("DROP TABLE {write_schema}.er_long_covid_table1_cohorts\n" )
# DBI::dbExecute(db, as.character(sql_query))
sql_query <- glue::glue("SELECT * INTO {write_schema}.er_long_covid_table1_cohorts\n",
                               "FROM (\n",
                               dbplyr::sql_render(table1_data_new_infections),
                               "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))

# add long covid symptoms , 28 days
table1_data_new_infections_28 <- get_long_cov_symptoms(main_cohort = cdm$er_long_covid_baseline_new_infections,
                                                       main_cohort_id = new_infection_id,
                                                       window_id  = 28)
# append rows to existing table 
add_sql_query_table1 <- function(tbl_df){
  sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_table1_cohorts\n",
                          "SELECT * FROM (\n",
                          dbplyr::sql_render(tbl_df),
                          "\n) AS from_table")
  DBI::dbExecute(db, as.character(sql_query)) 
}
add_sql_query_table1(table1_data_new_infections_28)


# get first infections / tests  and reinfections
get_n_infection <- function(main_table_1){
  table1 <- main_table_1 %>%
    left_join(cdm$er_long_covid_final_cohorts %>%
                filter(cohort_definition_id == first_infection_id) %>%
                select(-cohort_definition_id) %>%
                rename(person_id = subject_id) %>%
                mutate(infection = "first infection")) %>%
    mutate(infection = ifelse(is.na(infection), "reinfection", infection))
  table1
} 

# 90 days symptom cohorts 
table1_data_first_infection          <- get_n_infection(main_table_1 = table1_data_new_infections) %>% 
                                        filter(infection=="first infection") %>% select(-infection) %>%
                                        mutate(cohort_definition_id = first_infection_id)
                                            
table1_data_reinfections             <- get_n_infection(main_table_1 = table1_data_new_infections) %>%
                                        filter(infection=="reinfection") %>% select(-infection) %>%
                                        mutate(cohort_definition_id = reinfection_id)


add_sql_query_table1(table1_data_first_infection)
add_sql_query_table1(table1_data_reinfections) 

# 28 days symptom cohorts
table1_data_first_infection_28 <- get_n_infection(main_table_1 = table1_data_new_infections_28) %>% 
                                  filter(infection=="first infection") %>% select(-infection) %>%
                                  mutate(cohort_definition_id = first_infection_id)

table1_data_reinfections_28    <- get_n_infection(main_table_1 = table1_data_new_infections_28) %>% 
                                  filter(infection=="reinfection") %>% select(-infection) %>%
                                  mutate(cohort_definition_id = reinfection_id)
add_sql_query_table1(table1_data_first_infection_28)
add_sql_query_table1(table1_data_reinfections_28) 


# Tested negative cohorts 
# 90 days symptom cohort
table1_data_tested_negative_all <- get_long_cov_symptoms(main_cohort = cdm$er_long_covid_baseline_tested_negative_all ,
                                                         main_cohort_id = tested_negative_all_id ,
                                                         window_id  = 90)
add_sql_query_table1(table1_data_tested_negative_all)
# 28 days symptom cohort
table1_data_tested_negative_all_28 <- get_long_cov_symptoms(main_cohort = cdm$er_long_covid_baseline_tested_negative_all ,
                                                            main_cohort_id = tested_negative_all_id ,
                                                            window_id  = 28)
add_sql_query_table1(table1_data_tested_negative_all_28)

get_n_test <- function(main_table_1){
  table1 <- main_table_1 %>%
    left_join(cdm$er_long_covid_final_cohorts %>%
                filter(cohort_definition_id == tested_negative_earliest_id) %>%
                select(-cohort_definition_id) %>%
                rename(person_id = subject_id) %>%
                mutate(test = "first test")) %>%
    mutate(test = ifelse(is.na(test), "repeated tests", test))
  table1
} 

# earliest tested negative 
table1_data_tested_negative_earliest    <- get_n_test(main_table_1 = table1_data_tested_negative_all) %>%
                                           filter(test == "first test") %>% select(-test) %>%
                                            mutate(cohort_definition_id = tested_negative_earliest_id)
table1_data_tested_negative_earliest_28 <- get_n_test(main_table_1 = table1_data_tested_negative_all_28) %>%
                                           filter(test == "first test") %>% select(-test) %>%
                                           mutate(cohort_definition_id = tested_negative_earliest_id)

add_sql_query_table1(table1_data_tested_negative_earliest)
add_sql_query_table1(table1_data_tested_negative_earliest_28)


#### Generate  Matched cohort ----
cdm <- cdm_from_con(db, 
                    cdm_schema = cdm_database_schema,
                    write_schema = write_schema,
                    cohort_tables = c("er_long_covid_table1_cohorts")) # here are the cohorts with all symptoms

### New infections with tested negative  ----
table1_data_new_infections           <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == new_infection_id)
table1_data_first_infection          <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == first_infection_id)
table1_data_reinfections              <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == reinfection_id)
table1_data_tested_negative_earliest <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == tested_negative_earliest_id)
table1_data_tested_negative_all      <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == tested_negative_all_id)

# Matching cohorts by week of test  and 5y age band
matching_cohorts <- function(cohort1, cohort2, 
                             name1, name2, ratio, window_id) {
  results_list <- list()
  cohort_1 <- cohort1 %>% filter(window_days == window_id) %>% collect()    # cohort1 is the Exposed cohort
  cohort_2 <- cohort2 %>% filter(window_days == window_id) %>% collect()    # cohort2 is the control cohort
  
  data <- rbind(cohort_1 %>%  mutate(cohort = 1, name = name1) ,
                # comparator cohort
                cohort_2 %>% mutate(cohort =0,   name = name2)) %>%
   mutate(age_gr2 = factor(age_gr2,
                       levels = c("<=34",
                                  "35-49",
                                  "50-64",
                                  "65-79",
                                  ">=80"))) %>%
  mutate(trimester = factor(trimester,
                       levels = c("Sep-Dec 2020",
                                  "Jan-Apr 2021",
                                  "May-Aug 2021",
                                  "Sep-Dec 2021",
                                  "Jan-Apr 2022", 
                                  "May 2022 or later" 
                                  ))) %>%
    mutate(pcr = ifelse(is.na(pcr), 0, pcr)) %>%
  mutate(vaccination_status = factor(vaccination_status,
                              levels = c("Non vaccinated", "First dose vaccination", "Two doses vaccination", "Booster doses" ))) %>%
    mutate(age_gr5 = factor(age_gr5)) %>%
    droplevels() 
   
  

  # Matching - add PCR? ----
m.week_year <- matchit(cohort  ~ week + year + pcr + age_gr5, 
                        data   = data,
                        method = "nearest", 
                        exact  = ~ week + year + pcr + age_gr5,
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

### New infections with tested negative  ----
  
m.new_covid_tested_negative_earliest_90 <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  cohort2 = table1_data_tested_negative_earliest, 
  name2 = "Tested negative, earliest",
  ratio = 1,
  window_id = 90
)

m.new_covid_tested_negative_all_90 <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  cohort2 = table1_data_tested_negative_all, 
  name2 = "Tested negative, all",
  ratio = 1,
  window_id = 90
)

### First infections with reinfections ---- 
m.first_infection_reinfections_90 <- matching_cohorts(
  cohort1 = table1_data_reinfections,
  name1 = "Reinfections",
  cohort2 = table1_data_first_infection,
  name2 = "First infection",
  ratio = 3,
  window_id = 90
)

## Save matched datasets 
save(m.new_covid_tested_negative_earliest_90,
     m.new_covid_tested_negative_all_90,
     m.first_infection_reinfections_90,
     file = here("data", "table1_data_matched_90.Rdata"))

rm(m.new_covid_tested_negative_earliest_90, m.new_covid_tested_negative_all_90, m.first_infection_reinfections_90)

m.new_covid_tested_negative_earliest_28 <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  cohort2 = table1_data_tested_negative_earliest, 
  name2 = "Tested negative, earliest",
  ratio = 1,
  window_id = 28
)

m.new_covid_tested_negative_all_28 <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  cohort2 = table1_data_tested_negative_all, 
  name2 = "Tested negative, all",
  ratio = 1,
  window_id = 28
)

### First infections with reinfections ---- 
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
     m.new_covid_tested_negative_all_28,
     m.first_infection_reinfections_28,
     file = here("data", "table1_data_matched_28.Rdata"))

rm(m.new_covid_tested_negative_earliest_28, m.new_covid_tested_negative_all_28, m.first_infection_reinfections_28)


}
  
# Generate tables  ----
if(generate_results  =="FALSE"){
print(paste0("Skipping  generating descriptive tables"))
} else { 
print(paste0("Generating descriptive tables"))
  
  # cdm to get the cohorts for Table 1 - unmatched cohorts 
  cdm <- cdm_from_con(db, 
                      cdm_schema = cdm_database_schema,
                      write_schema = write_schema,
                      cohort_tables = c("er_long_covid_table1_cohorts")) # here are the cohorts with all symptoms
  # functions for numbers
  nice.num<-function(x){
    prettyNum(x, big.mark=",", nsmall = 0, digits=0,scientific = FALSE)
  }
  
  # variables of interest 
  symptoms <- c("abdominal_pain", "allergy", "altered_smell_taste",  "anxiety" , 
                "blurred_vision","chest_pain" ,"cognitive_dysfunction_brain_fog", "cough" ,"depression" ,
                "dizziness" ,"dyspnea" , "fatigue_malaise" , "gastrointestinal_issues","headache" ,"intermittent_fever" ,
                "joint_pain",  "memory_issues", "menstrual_problems", "muscle_spams_pain", "neuralgia", "pins_sensation", 
                "postexertional_fatigue",    "sleep_disorder", "tachycardia" ,"tinnitus_and_hearing_problems")
  
 other_factor_vars <- c( "age_gr2",
                         "sex",
                         "trimester",
                         "vaccination_status",
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
 
 factor.vars <- c(other_factor_vars, symptoms, "all_symp")
 rm(other_factor_vars)
   
# all variables - (includes continous variables)
vars <- c("age", factor.vars)
  
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
    rownames(summary_characteristics)<-str_to_sentence(rownames(summary_characteristics))
    rownames(summary_characteristics)<-str_replace(rownames(summary_characteristics) , "Copd", "COPD")
    rownames(summary_characteristics)<-str_replace(rownames(summary_characteristics) , "Sex", "Sex, male")
    summary_characteristics
  }

# function to get all the descriptive tables  
get_descriptive_tables <- function(window_id){ 

  # Get table1 data for each cohort of interest
  table1_data_new_infections           <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == new_infection_id   & window_days == window_id) %>% collect()
  table1_data_first_infection          <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == first_infection_id & window_days == window_id) %>% collect()
  table1_data_reinfections             <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == reinfection_id     & window_days == window_id) %>% collect()
  table1_data_tested_negative_earliest <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == tested_negative_earliest_id & window_days == window_id) %>% collect()
  table1_data_tested_negative_all      <- cdm$er_long_covid_table1_cohorts %>% filter(cohort_definition_id == tested_negative_all_id      & window_days == window_id) %>% collect()
  
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
      mutate(group = "Tested negative, all")
  ) 
  
  summary.characteristics <-print(CreateTableOne(
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
    printToggle=FALSE)
  
  for(i in 1:5) {
    # tidy up 
    cur_column <- summary.characteristics[, i]
    cur_column <- str_extract(cur_column, '[0-9.]+\\b') %>% 
      as.numeric() 
    cur_column <-nice.num(cur_column)
    # add back in
    summary.characteristics[, i] <- str_replace(string=summary.characteristics[, i],
                                                pattern='[0-9.]+\\b', 
                                                replacement=cur_column)    
  }
  
  rownames(summary.characteristics)<- str_replace(rownames(summary.characteristics), "age_gr2", "Age, categories")
  rownames(summary.characteristics)<- str_replace(rownames(summary.characteristics), " = 1", "")
  rownames(summary.characteristics)<- str_replace(rownames(summary.characteristics), "a_", "")
  rownames(summary.characteristics)<- str_replace_all(rownames(summary.characteristics) , "_", " ")
  rownames(summary.characteristics)<- str_to_sentence(rownames(summary.characteristics))
  rownames(summary.characteristics)<- str_replace(rownames(summary.characteristics) , "Copd", "COPD")
  rownames(summary.characteristics)<- str_replace(rownames(summary.characteristics) , "Sex", "Sex, female")
  
  # save Table 1
  write.csv2(summary.characteristics, file = here(paste0("results/Table1_", window_id, "days_", database_name, ".csv")))
  
  # Comparing characteristics of matched infections vs tested negative, earliest
m.covid_tested_neg_earliest   <-  get(paste0("m.new_covid_tested_negative_earliest_", window_id))[[3]]    
m.covid_tested_neg_all         <-  get(paste0("m.new_covid_tested_negative_all_", window_id))[[3]]
m.first_infection_reinfections <-  get(paste0("m.first_infection_reinfections_", window_id))[[3]]
  
  sum_characteristics_covid_negative_earliest <- get_characteristics_matched(df = m.covid_tested_neg_earliest)
  sum_characteristics_covid_negative_all      <- get_characteristics_matched(df = m.covid_tested_neg_all)
  sum_characteristics_reinfections            <- get_characteristics_matched(df = m.first_infection_reinfections) 
  
  write.csv2(sum_characteristics_covid_negative_earliest, 
             file = here(paste0("results/Table_Matched_Infection_Tested_Negative_earliest_", window_id, "days_", database_name, ".csv"))
             )
  write.csv2(sum_characteristics_covid_negative_all, 
             file = here(paste0("results/Table_Matched_Infection_Tested_Negative_all_", window_id, "days_", database_name, ".csv"))
             )
  write.csv2(sum_characteristics_reinfections, 
             file = here(paste0("results/Table_Matched_First_infection_Reinfections_", window_id, "days_", database_name, ".csv"))
             )
  }


# function to get RR and AR ----
  get_rr <- function(data){
    results_rr<- list()
    # for each symptom
    for (i in 1:length(symptoms)){
      symptom_id <-symptoms[i]
      message(paste0("working on ", symptom_id))
      # get number of events & number of people per cohort
      counts_1 <- data %>%
        mutate(symptom =paste0(symptom_id)) %>%
        rename(name_1 = name) %>%
        filter(cohort==1) %>%
        mutate(total_pop_1 = length(person_id)) %>% # get total pop
        filter((!!sym(symptom_id))==1) %>%
        mutate(events_1= length(person_id))%>% # get number of people with the symtpom
        select(symptom, name_1, events_1, total_pop_1) %>% 
        distinct()  %>%
        mutate(prop_1 = (events_1/total_pop_1)*100)  # get proportion of people with the symtpmm
      
      counts_2<- data %>%
        mutate(symptom =paste0(symptom_id)) %>%
        rename(name_2 = name) %>%
        filter(cohort==0) %>%
        mutate(total_pop_2 = length(person_id)) %>% 
        filter((!!sym(symptom_id))==1) %>%
        mutate(events_2= length(person_id))%>% 
        select(symptom, name_2, events_2, total_pop_2) %>% 
        distinct()  %>%
        mutate(prop_2 = (events_2/total_pop_2)*100) 
      
      data_rr <- counts_1 %>%
        left_join(counts_2) %>%
        mutate(database = database_name) %>%
        mutate(symptom = str_to_sentence(str_replace_all(symptom, "_", " "))) 
      
      # getting relative risl with 95%Ci
      getting_rr <- riskratio(data_rr$events_1,
                              data_rr$events_2,
                              data_rr$total_pop_1,
                              data_rr$total_pop_2,
                              conf.level = 0.95)
      
      data_rr$relative_risk <- getting_rr$estimate
      data_rr$low_ci <- getting_rr$conf.int[1]
      data_rr$up_ci <- getting_rr$conf.int[2]
      
      # getting absolute risk difference with 95%C
      getting_ar <- BinomDiffCI(
        data_rr$events_1,   # events exposed
        data_rr$total_pop_1,# pop exposed
        data_rr$events_2,
        data_rr$total_pop_2,
        conf.level = 0.95, 
        sides = "two.sided",
        method = "wald")
      
      data_rr$absolute_risk <- getting_ar[1]*100
      data_rr$ar_low_ci <- getting_ar[2]*100
      data_rr$ar_up_ci <- getting_ar[3]*100
      
      data_rr
      results_rr[[i]] <- data_rr      
    }
    results_rr
  }
  
  save_rr <- function(window_id){
    m.covid_tested_neg_earliest    <-  get(paste0("m.new_covid_tested_negative_earliest_", window_id))[[2]]    
    m.covid_tested_neg_all         <-  get(paste0("m.new_covid_tested_negative_all_", window_id))[[2]]
    m.first_infection_reinfections <-  get(paste0("m.first_infection_reinfections_", window_id))[[2]]
    
    rr_new_covid_tested_negative_earliest <- bind_rows(get_rr(m.covid_tested_neg_earliest)) 
    rr_new_covid_tested_negative_all      <- bind_rows(get_rr(m.covid_tested_neg_all)) 
    rr_first_infection_reinfections       <- bind_rows(get_rr(m.first_infection_reinfections))
    
    write.csv2(rr_new_covid_tested_negative_earliest,
               here(paste0("results/RR_new_covid_tested_negative_earliest_", window_id, "days_",
                           database_name, 
                           ".csv")),
               row.names = FALSE)
    
    write.csv2(rr_new_covid_tested_negative_all,
               here(paste0("results/RR_new_covid_tested_negative_all_", window_id, "days_",
                           database_name,
                           ".csv")),
               row.names = FALSE)
    
    write.csv2(rr_first_infection_reinfections,
               here(paste0("results/RR_first_infection_reinfections_", window_id, "days_",
                           database_name,
                           ".csv")),
               row.names = FALSE)
  }
  
  get_descriptive_tables(window_id = 90) 
  get_descriptive_tables(window_id = 28) 
  
  save_rr(window_id = 90)
  save_rr(window_id = 28)
  
  
  # get exclusion counts ----
stats           <- read.csv(here("Cohort_Dx_initial/Results/cohortIncStats.csv"))
label_inclusion <- read.csv2(here("inclusion_criteria_labels.csv"))
  read.csv2(here("results/exclusion_table_CPRD.csv"))

  
  
  
  
  
  
} 


# print("Zipping results to output folder")
# unlink(paste0(output.folder, "/OutputToShare_", db.name, ".zip"))
# zipName <- paste0(output.folder, "/OutputToShare_", db.name, ".zip")
# 
if (generate_results == TRUE){
  
}
# if(run.vax.cohorts==TRUE | run.covid.cohorts==TRUE){
#   files<-c(paste0(output.folder, "/IR.summary_", db.name, ".RData"),
#            paste0(output.folder, "/Survival.summary_", db.name, ".RData"),
#            paste0(output.folder, "/Patient.characteristcis_", db.name, ".RData"))
# } else {
#   files<-c(paste0(output.folder, "/IR.summary_", db.name, ".RData"),
#            paste0(output.folder, "/Patient.characteristcis_", db.name, ".RData")) 
# }
# files <- files[file.exists(files)==TRUE]
# createZipFile(zipFile = zipName,
#               rootFolder=output.folder,
#               files = files)

print("Done!")
print("-- If all has worked, there should now be a zip folder with your results in the results folder to share")
print("-- Thank you for running the study!")


