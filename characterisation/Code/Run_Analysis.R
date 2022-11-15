# Tables in the write_schema to save Inital Json cohorts
cohortTableJsons <-"er_cohorts_for_longcov"


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


# Initial Json cohorts ----
if(create_initial_json_cohorts=="FALSE"){
print(paste0("- Skipping creating initial json cohorts"))
} else { 
print(paste0("- Creating initial json cohorts"))


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


## Cohort ids -----
# Cohort ids are in a csv with all cohorts ids
cohorts_ids  <- read.csv2(here("ATLAS Cohort Definitions_LongCovid.csv")) %>%
                          mutate(name= paste(sub(".*LongCov-ER_", "", name)))
covid_ids    <- cohorts_ids %>% filter(type=="covid-19") %>% select(cohort_definition_id) %>% pull()
influenza_id <- cohorts_ids %>% filter(type=="influenza") %>% select(cohort_definition_id) %>% pull()
controls_ids <- cohorts_ids %>% filter(type=="control") %>% select(cohort_definition_id) %>% pull()
symptoms_ids <- cohorts_ids %>% filter(type=="symptom") %>% select(cohort_definition_id) %>% pull()

# combine observation period (to have observation period end date) and death with our study cohorts
observation_death <- cdm$observation_period %>%
  select(person_id, observation_period_end_date) %>%
  left_join(cdm$death %>% select (person_id, death_date)) %>%
  mutate(death = ifelse(!(is.na(death_date)), 1,0))

cohorts <- cdm$er_cohorts_for_longcov %>%
  left_join(observation_death, by = c("subject_id" = "person_id") )

## get initial cohorts ----
# influenza cohort - to use for censoring 
influenza_cohort <- cohorts %>%
  filter(cohort_definition_id %in% influenza_id) %>%
  filter(cohort_start_date>=influenza_start_date) %>%
  select(cohort_start_date, subject_id) %>%
  rename(influenza_start_date= cohort_start_date) %>%
  compute() 

covid_cohorts <- cohorts %>%
 filter(cohort_definition_id %in% covid_ids) %>%
 filter(cohort_start_date>=covid_start_date) %>%
 select(-cohort_end_date) %>%
 compute()  

control_cohorts <- cohorts %>%
 filter(cohort_definition_id %in% controls_ids) %>%
 filter(cohort_start_date>=covid_start_date) %>%
  select(-cohort_end_date) %>%
 compute()

symptom_cohorts <- cohorts %>%
 filter(cohort_definition_id %in% symptoms_ids) %>%
 filter(cohort_start_date>=symptoms_start_date) %>%
 select(-c(cohort_end_date, observation_period_end_date,death_date, death)) %>%
 rename(symptom_date = cohort_start_date) %>%
 rename(symptom_definition_id = cohort_definition_id) %>%
 select(symptom_definition_id, subject_id, symptom_date) %>%
 compute()

rm(covid_ids, influenza_id, controls_ids, cohorts, observation_death)


####################################### Getting cohorts ########

## COVID-19 cohorts ----
###  Covid infection - diagnoses and or laboratory tests  ----
new_infection_id <- cohorts_ids %>%
                    filter(str_detect(name, "New_Infection")) %>% 
                    select(cohort_definition_id) %>% pull()

covid_infection <- covid_cohorts %>%
  filter(cohort_definition_id==new_infection_id)%>%
  arrange(subject_id, desc(as.Date(cohort_start_date))) %>% 
  group_by(subject_id) %>% 
  arrange(cohort_start_date) %>%
  mutate(seq=row_number()) %>% 
  distinct() %>%
  ungroup() %>%
  compute()

rm(new_infection_id)

# add next infection date to use for censoring 
covid_infection <- covid_infection %>%
 arrange(subject_id,cohort_start_date) %>% 
 group_by(subject_id) %>%
 mutate(covid_next_inf_date = lead(cohort_start_date)) %>%
 ungroup() %>%
 compute()

# add influenza to censor
### first we get rid of influenzxa dx prior to cohort_start_date
influenza_covid <- influenza_cohort %>%
  inner_join(covid_infection) %>%
  filter(influenza_start_date>cohort_start_date) %>%
  distinct() %>%
  compute()

# we check if we have people with more than one influenza infection after cohort start  
# it's cohorts in which all columns are identical aside from influenza_date
repeated_influenza <-  influenza_covid %>%
  group_by_at(vars(-influenza_start_date)) %>% 
  filter(n() > 1) %>%
  ungroup() %>%
  compute()

# have_look <- repeated_influenza %>%collect()

# we keep the first influenza record
repeated_influenza <- repeated_influenza %>%
  group_by(subject_id) %>%
  arrange(influenza_start_date) %>%
  mutate(seq2= row_number()) %>%
  ungroup() %>%
  filter(seq2==1) %>%
  select(-seq2) %>%
  compute()

## there is a warning about ordering but it seems to be working fine... 
# have_look_2 <- repeated_influenza %>%collect()
# have_look_2
# rm(have_look, have_look_2)

# we exclude these subjects from influenza covid
influenza_covid <- influenza_covid %>%
anti_join(repeated_influenza %>% select(subject_id) %>% distinct())
# we add them again with the clean repeated influenza
influenza_covid <- union_all(influenza_covid, repeated_influenza)

# similarly, we exclude all people with an influenza dx during our study period from the main df
covid_infection <- covid_infection %>%
anti_join(influenza_covid %>% select(subject_id) %>% distinct()) %>%
  mutate(influenza_start_date=as.Date(NA))%>%
  compute()
# we add them again with the cleaned df for influenza
covid_infection <- union_all(covid_infection, influenza_covid)
rm(influenza_covid,  repeated_influenza)

# add date one year after infection
covid_infection <- covid_infection %>%
mutate(one_year_date = cohort_start_date+lubridate::days(365))%>%
compute()

# add cohort end date, considering censoring options
# death, observation end, next covid infection, influenza, one year followup

covid_infection <- covid_infection %>% 
  mutate(cohort_end_date = lubridate::as_date(pmin(observation_period_end_date, 
                                death_date, 
                                covid_next_inf_date-lubridate::days(1), # trec un dia
                                # pq si no una el cohort_end_date de la persona amb reinifeccion
                                # es igual q el cohort_start_date de la seguent infeccio
                                influenza_start_date-lubridate::days(1), 
                                one_year_date,
                                na.rm = TRUE))) %>%
  mutate(follow_up_days = cohort_end_date - cohort_start_date) %>% 
  compute()

covid_infection <- covid_infection %>% 
  select(cohort_definition_id, subject_id, cohort_start_date, 
         cohort_end_date, seq, follow_up_days) %>%
  compute()

# we check numbers make sense -
#covid_infection %>% tally()  # 1183355 infections
## check that follow_up_days make sense
#summary(covid_infection %>% select(follow_up_days) %>% distinct() %>%collect()) 

# keep only cohort start dates prior to the last admissible index date (2nd september for SIDIAP)
covid_infection <- covid_infection %>%
  filter(cohort_start_date<=end_index_date) %>%
  compute()

#covid_infection %>%tally()  # 912829

# we exclude people with less than 120 days of follow-up
exclusion_table <- tibble(N_current=covid_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="initial pop",
                          cohort_definition_id = covid_infection %>%
                                                 select(cohort_definition_id) %>% 
                            distinct() %>%pull())

covid_infection <- covid_infection %>%
  filter(!(follow_up_days<120))%>%
  select(-follow_up_days) %>%
  compute()
#check follow-up days are fine
# summary(covid_infection %>% select(follow_up_days) %>% distinct() %>%collect()) 

exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=covid_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="Less than 120 days of follow-up",
                          cohort_definition_id = covid_infection %>%
                                                 select(cohort_definition_id) %>% 
                            distinct() %>%pull()
                         ))


# covid_infection %>%tally()  #857732 infections
# covid_infection %>% select(subject_id)%>% distinct() %>%tally() #805884 subjects

### First COVID-19 infection 
first_infection <- covid_infection %>%
  filter(seq==1) %>% 
  distinct() %>%
  select(-seq) %>%
  # we change cohort definition id so that is has its own id
  mutate(cohort_definition_id=as.integer(70)) %>%
  compute() 
# check we have one row per person
# first_infection %>%tally()    # 782948
# first_infection %>% select(subject_id)%>% distinct() %>%tally() #782948

# we add the cohort id to our cohorts ids table
cohorts_ids <- rbind(cohorts_ids,
                     tibble(atlas_id = NA,
                            name = "First COVID-19 infection",
                            cohort_definition_id =70, 
                            name_var = "first_infection",
                            type = "covid-19"))

exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=first_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="initial pop",
                          cohort_definition_id = first_infection%>%
                                                  select(cohort_definition_id) %>% 
                            distinct() %>%pull()
                         ))


reinfections <- covid_infection %>%
  filter(seq!=1)%>% 
  select(-seq) %>%
  distinct() %>%
  # we change cohort definition id so that is has its own id
  mutate(cohort_definition_id=as.integer(71)) %>%
  compute()

# reinfections %>%tally()    #  74784
reinfections %>% select(subject_id)%>% distinct() %>%tally() #70752

exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=reinfections %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="initial pop",
                          cohort_definition_id = reinfections %>%
                                                 select(cohort_definition_id) %>%
                            distinct() %>%pull()
                          ))
# we add the cohort identificator to our cohorts ids table
cohorts_ids <- rbind(cohorts_ids,
                     tibble(atlas_id = NA,
                            name = "Reinfections",
                            cohort_definition_id =71, 
                            name_var = "reinfections",
                            type = "covid-19"))

covid_infection <- covid_infection %>%
 select(-seq)%>%
  compute()

covid_infection <- covid_infection %>%
  mutate(cohort_definition_id= as.integer(cohort_definition_id))

## inserting these cohorts into the database
#### remove prior tables if needed
 sql_query <- glue::glue("DROP TABLE {write_schema}.er_long_covid_final_cohorts\n" )
 DBI::dbExecute(db, as.character(sql_query))

 # create the new table 
sql_query <- glue::glue("SELECT * INTO {write_schema}.er_long_covid_final_cohorts\n",
                          "FROM (\n",
                          dbplyr::sql_render(reinfections),
                          "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))

# append rows to existing table
sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
                          "SELECT * FROM (\n",
                          dbplyr::sql_render(covid_infection),
                          "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))

sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
                          "SELECT * FROM (\n",
                          dbplyr::sql_render(first_infection),
                          "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))

## aixo no cal q ho miris pq es el mateix q el codi d'abans - per tant el q estigui malament abans 
# també ho està aquí :D
###  Tested positive  ----
confirmed_infection_id <- cohorts_ids %>% filter(str_detect(name, "Tested_Positive")) %>%select(cohort_definition_id) %>% pull()

confirmed_infection <- covid_cohorts %>%
  filter(cohort_definition_id==confirmed_infection_id) %>%# change this for confirmed infections
  arrange(subject_id, desc(as.Date(cohort_start_date))) %>% 
  group_by(subject_id) %>% 
  arrange(cohort_start_date) %>%
  mutate(seq=row_number()) %>% 
  distinct() %>%
  ungroup() %>%
  compute()

rm(confirmed_infection_id)
# add next infection date to use for censoring 
confirmed_infection <- confirmed_infection %>%
 arrange(subject_id,cohort_start_date) %>% 
 group_by(subject_id) %>%
 mutate(covid_next_inf_date = lead(cohort_start_date)) %>%
 ungroup() %>%
 compute()

# add influenza to censor
### first we get rid of influenzxa dx prior to cohort_start_date
influenza_covid <- influenza_cohort %>%
  inner_join(confirmed_infection) %>%
  filter(influenza_start_date>cohort_start_date) %>%
  distinct() %>%
  compute()

# we check if we have people with more than one influenza infection after cohort start  
# it's cohorts in which all columns are identical aside from influenza_date
# we don't have for confirmed infectiosn but we keep the code 
repeated_influenza <-  influenza_covid %>%
  group_by_at(vars(-influenza_start_date)) %>% 
  filter(n() > 1) %>%
  ungroup() %>%
  compute()

# we keep the first influenza record
repeated_influenza <- repeated_influenza %>%
  group_by(subject_id) %>%
  arrange(influenza_start_date) %>%
  mutate(seq2= row_number()) %>%
  ungroup() %>%
  filter(seq2==1) %>%
  select(-seq2) %>%
  compute()

# we exclude these subjects from influenza covid
influenza_covid <- influenza_covid %>%
  anti_join(repeated_influenza %>%select(subject_id) %>% distinct())
# we add them again with the clean repeated influenza
influenza_covid <- union_all(influenza_covid, repeated_influenza)

# similarly, we exclude all people with an influenza dx during our study period from the main df
confirmed_infection <- confirmed_infection %>%
  anti_join(influenza_covid %>% select(subject_id) %>% distinct()) %>%
  mutate(influenza_start_date=as.Date(NA))%>%
  compute()
# we add them again with the cleaned df for influenza
confirmed_infection <- union_all(confirmed_infection, influenza_covid)
rm(influenza_covid,  repeated_influenza)

# add date one year after infection
confirmed_infection <- confirmed_infection %>%
mutate(one_year_date = cohort_start_date+lubridate::days(365))%>%
compute()

# add cohort end date, considering censoring options
# death, observation end, next covid infection, influenza, one year followup
confirmed_infection <- confirmed_infection %>% 
  mutate(cohort_end_date =lubridate::as_date(pmin(observation_period_end_date, 
                                death_date, 
                                covid_next_inf_date-lubridate::days(1), # trec un dia
                                # pq si no una el cohort_end_date de la persona amb reinifeccion
                                # es igual q el cohort_start_date de la seguent infeccio
                                influenza_start_date, 
                                one_year_date,
                                na.rm = TRUE))) %>%
  mutate(follow_up_days = (cohort_end_date - cohort_start_date))  %>%
  select(cohort_definition_id, subject_id, cohort_start_date, cohort_end_date, seq, follow_up_days) %>%
  compute()



# we check numbers make sense -
# confirmed_infection %>% tally()  # 618080 infections
# ## check that follow_up_days make sense
# summary(confirmed_infection %>% select(follow_up_days) %>% distinct() %>%collect()) 

# keep only cohort start dates prior to the last admissible index date (2nd september for SIDIAP)
confirmed_infection <- confirmed_infection %>%
  filter(cohort_start_date<=end_index_date) %>%
  compute()
# 
# confirmed_infection %>%tally()  # 452647

# we exclude people with less than 120 days of follow-up
exclusion_table <- rbind(exclusion_table,
                         tibble(N_current=confirmed_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="initial pop",
                          cohort_definition_id = confirmed_infection %>%
                            select(cohort_definition_id) %>% distinct() %>%pull()
                         ))

confirmed_infection <- confirmed_infection %>%
  filter(!(follow_up_days<120))%>%
  select(-follow_up_days, -seq) %>%
  compute()
#check follow-up days are fine
# summary(confirmed_infection %>% select(follow_up_days) %>% distinct() %>%collect()) 


exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=confirmed_infection %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="Less than 120 days of follow-up",
                         cohort_definition_id = confirmed_infection %>%
                            select(cohort_definition_id) %>% distinct() %>%pull()
                        ))

# 
# confirmed_infection %>%tally()  #431571infections
# confirmed_infection %>% select(subject_id)%>% distinct() %>%tally() #425423subjects
sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
                          "SELECT * FROM (\n",
                          dbplyr::sql_render(confirmed_infection),
                          "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))


## Tested negative cohorts 
### PCR and antigen negative _all events 

# covid infections to use for censoring
new_infection_id  <- cohorts_ids %>%
                    filter(str_detect(name, "New_Infection")) %>% 
                    select(cohort_definition_id) %>% pull()

covid_infection_cens <- covid_cohorts %>%
  filter(cohort_definition_id==new_infection_id ) %>%
  arrange(subject_id, desc(as.Date(cohort_start_date))) %>% 
  group_by(subject_id) %>% 
  arrange(cohort_start_date) %>%
  distinct() %>%
  ungroup() %>%
  rename(covid_infection_date = cohort_start_date) %>%
  select(subject_id, covid_infection_date)%>%
  compute()



# function to generate tested negative cohorts 
generate_tested_negative_cohort <- function(id_interest){
  # get the cohort of interst
 working_cohort <- control_cohorts %>%
  filter(cohort_definition_id== id_interest) %>%
  compute()
 
  message(paste0("working on cleaning Covid-19 infections"))

 # first, clean covid infectiosn for censoring -- people can have more than infection recorded
 covid_negative <- covid_infection_cens %>%
  inner_join(working_cohort) %>%
  distinct() %>%
  compute()
 
# we check if we have people with more than one covid infectoin after cohort start  
# it's cohorts in which all columns are identical aside from covid_infection_date
repeated_covid <-  covid_negative %>%
  group_by_at(vars(-covid_infection_date)) %>% 
  filter(n() > 1) %>%
  ungroup() %>%
  compute()

# we keep the first covid record
repeated_covid <- repeated_covid %>%
  group_by(subject_id) %>%
  arrange(covid_infection_date) %>%
  mutate(seq= row_number()) %>%
  ungroup() %>%
  filter(seq==1) %>%
  select(-seq) %>%
  compute()

# we exclude these subjects from covid_negative

covid_negative <- covid_negative %>%
  anti_join(repeated_covid %>% select(subject_id) %>% distinct())
# we add them again with the clean repeated covid
covid_negative <- union_all(covid_negative, repeated_covid)
# similarly, we exclude all people with a covid dx during our study period from the main df
working_cohort <- working_cohort %>%
    anti_join(covid_negative %>% select(subject_id) %>% distinct()) %>%
  mutate(covid_infection_date=as.Date(NA))%>%
  compute()
# we add them again with the cleaned df for covid
working_cohort <- union_all(working_cohort, covid_negative)

  message(paste0("working on cleaning influenza infections"))
## same proces now for influenza - but we also get rid of prior influenza diagnoses
influenza_covid <- influenza_cohort %>%
  inner_join(working_cohort) %>%
  filter(influenza_start_date>cohort_start_date) %>%
  distinct() %>%
  compute()

# we check if we have people with more than one influenza infection after cohort start  
# it's cohorts in which all columns are identical aside from influenza_date
repeated_influenza <-  influenza_covid %>%
  group_by_at(vars(-influenza_start_date)) %>% 
  filter(n() > 1) %>%
  ungroup() %>%
  compute()

# we keep the first influenza record
repeated_influenza <- repeated_influenza %>%
  group_by(subject_id) %>%
  arrange(influenza_start_date) %>%
  mutate(seq2= row_number()) %>%
  ungroup() %>%
  filter(seq2==1) %>%
  select(-seq2) %>%
  compute()

# we exclude these subjects from influenza covid
influenza_covid <- influenza_covid %>%
  anti_join(repeated_influenza %>% select(subject_id)%>% distinct())
# we add them again with the clean repeated influenza
influenza_covid <- union_all(influenza_covid, repeated_influenza)

# similarly, we exclude all people with an influenza dx during our study period from the main df
working_cohort <- working_cohort %>%
  anti_join(influenza_covid %>% select(subject_id)%>% distinct()) %>%
  mutate(influenza_start_date=as.Date(NA))%>%
  compute()
# we add them again with the cleaned df for influenza
working_cohort <- union_all(working_cohort, influenza_covid)

# add date one year after infection
working_cohort <- working_cohort %>%
mutate(one_year_date = cohort_start_date+lubridate::days(365))%>%
compute()

# add cohort end date, considering censoring options
# death, observation end, next covid infection, influenza, one year followup
working_cohort <- working_cohort %>% 
  mutate(cohort_end_date = lubridate::as_date(pmin(observation_period_end_date, 
                                death_date, 
                                covid_infection_date-lubridate::days(1), # trec un dia
                                influenza_start_date, 
                                one_year_date,
                                na.rm = TRUE))) %>%
  mutate(follow_up_days = cohort_end_date - cohort_start_date) %>% 
  select(cohort_definition_id, subject_id, cohort_start_date, 
         cohort_end_date, follow_up_days) %>%
  compute()

  message(paste0("working on excluding people without sufficent follow-up"))
# keep only cohort start dates prior to the last admissible index date (2nd september for SIDIAP)
working_cohort <- working_cohort %>% 
  filter(cohort_start_date<=end_index_date) %>%
  compute()

# we exclude people with less than 120 days of follow-up
exclusion_table <- tibble(N_current=working_cohort %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="initial pop",
                          cohort_definition_id = working_cohort %>%
                                                 select(cohort_definition_id) %>% 
                          distinct() %>%pull()
                          )

working_cohort <- working_cohort %>% 
  filter(!(follow_up_days<120))%>%
  select(-follow_up_days) %>%
  compute()

exclusion_table<-rbind(exclusion_table,
                       tibble(N_current=working_cohort  %>%tally()%>%collect()%>%pull(), 
                          exclusion_reason="Less than 120 days of follow-up",
                          cohort_definition_id = working_cohort  %>%
                                                 select(cohort_definition_id) %>% 
                            distinct() %>%pull()
                         ))

sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
                          "SELECT * FROM (\n",
                          dbplyr::sql_render(working_cohort),
                          "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))

result <- list(working_cohort, exclusion_table) 
result

}

# get the ids of the tested negative cohorts
tested_negative_all_id <- cohorts_ids %>% 
                          filter(str_detect(name, "Tested_Negative_all")) %>%
                          select(cohort_definition_id) %>% pull()

tested_negative_earliest_id <- cohorts_ids %>% 
                          filter(str_detect(name, "Tested_Negative_earliest")) %>%
                          select(cohort_definition_id) %>% pull()

PCR_negative_earliest_id <- cohorts_ids %>% 
                          filter(str_detect(name, "PCR_Negative_earliest")) %>%
                          select(cohort_definition_id) %>% pull()
PCR_negative_all_id <- cohorts_ids %>% 
                          filter(str_detect(name, "PCR_Negative_all")) %>%
                          select(cohort_definition_id) %>% pull()

tested_negative_earliest <- generate_tested_negative_cohort(id_interest = tested_negative_earliest_id)
tested_negative_all      <- generate_tested_negative_cohort(id_interest = tested_negative_all_id)
PCR_negative_earliest    <- generate_tested_negative_cohort(id_interest = PCR_negative_earliest_id)
PCR_negative_all         <- generate_tested_negative_cohort(id_interest = PCR_negative_all_id)

rm(new_infection_id, tested_negative_all_id, tested_negative_earliest_id, PCR_negative_earliest_id,PCR_negative_all_id, covid_infection_cens)

exclusion_table <- bind_rows(exclusion_table,
                              tested_negative_earliest[[2]],
                              tested_negative_all[[2]],
                              PCR_negative_earliest[[2]],
                              PCR_negative_all[[2]]
                              ) %>%
  left_join(cohorts_ids %>% select(cohort_definition_id, name, type) %>%
              mutate(cohort_definition_id=as.integer(cohort_definition_id)))

#### save tested negative cohorts 
# # canviar el codi per fer-ho dincs la funcio 
# sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
#                           "SELECT * FROM (\n",
#                           dbplyr::sql_render(tested_negative_earliest[[1]]),
#                           "\n) AS from_table")
# DBI::dbExecute(db, as.character(sql_query))
# sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
#                           "SELECT * FROM (\n",
#                           dbplyr::sql_render(tested_negative_all[[1]]),
#                           "\n) AS from_table")
# DBI::dbExecute(db, as.character(sql_query))
# sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
#                           "SELECT * FROM (\n",
#                           dbplyr::sql_render(PCR_negative_earliest[[1]]),
#                           "\n) AS from_table")
# DBI::dbExecute(db, as.character(sql_query))
# sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
#                           "SELECT * FROM (\n",
#                           dbplyr::sql_render(PCR_negative_all[[1]]),
#                           "\n) AS from_table")
# DBI::dbExecute(db, as.character(sql_query))


## Symptoms  ----
### parameters for loop
symptoms_ids <- cohorts_ids %>% filter(type=="symptom") %>% select(cohort_definition_id) %>% pull()
window_longCov <- c(28,90)

cohorts_list <- list()
excluded_list <- list()
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
  mutate(trimester=ifelse(year==2020 & month<4, "Jan_Mar_2020", 
                   ifelse(year==2020 & (month==4 | month==5| month==6), "Apr_Jun_2020", 
                   ifelse(year==2020 & (month==7 | month==8| month==9), "Jul_Sep_2020",
                   ifelse(year==2020 & month>9, "Oct_Dec_2020",
                   ifelse(year==2021 & month<4, "Jan_Mar_2021",
                   ifelse(year==2021 & (month==4 | month==5| month==6), "Apr_Jun_2021",
                   ifelse(year==2021 & (month==7 | month==8| month==9), "Jul_Sep_2021",
                   ifelse(year==2021 & month>9, "Oct_Dec_21", 
                   NA))))))))) %>%
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
# we insert this into the database
sql_query <- glue::glue("INSERT INTO {write_schema}.er_long_covid_final_cohorts\n",
                          "SELECT * FROM (\n",
                          dbplyr::sql_render(working_symptom),
                          "\n) AS from_table")
DBI::dbExecute(db, as.character(sql_query))

working_symptom <- working_symptom %>%
collect
# we also save this into a list to generaste the any symptom cohort
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

## function to generate any sypmtom cohort
# aixo s'ha de millorar per fer-ho sense necessitat d'haver fet un collect abans
create_any_symptom <- function(list_cohorts){
# we first bind the rows from the previous function result
any_symp_cohort <- bind_rows(list_cohorts[1]) %>%
# we generate a new cohort_definition_id -- for any symtpom we use 99
                   mutate(cohort_definition_id = 
                        (as.numeric(substr(cohort_definition_id, 1, 2))*10^4) +
                         99*10^2+
                         as.numeric(substr(cohort_definition_id, 
                         nchar(cohort_definition_id[1])-1, 
                         nchar(cohort_definition_id[1])))) %>%
                  distinct() %>%
  group_by(subject_id, cohort_definition_id, cohort_start_date) %>%
  arrange(cohort_end_date) %>% 
  # we keep just first symptom recorded by unique cohort, subject id and cohort start id
  mutate(seq=row_number()) %>%
  filter(seq==1) %>%
  select(-seq)%>%
  ungroup()
}



# Covid cohorts & control cohorts 
covid_infection_symp          <- creating_symptom_cohorts(cohort=covid_infection)
first_infection_symp          <- creating_symptom_cohorts(cohort=first_infection)
confirmed_infection_symp      <- creating_symptom_cohorts(cohort=confirmed_infection)
reinfections_symp             <- creating_symptom_cohorts(cohort= reinfections)
tested_negative_earliest_symp <- creating_symptom_cohorts(cohort= tested_negative_earliest[[1]] )
tested_negative_all_symp      <- creating_symptom_cohorts(cohort= tested_negative_all[[1]])
PCR_negative_earliest_symp    <- creating_symptom_cohorts(cohort= PCR_negative_earliest[[1]])
PCR_negative_all_symp         <- creating_symptom_cohorts(cohort= PCR_negative_all[[1]])


# denominator de reinfection has one column missing (because for the first 3 months period there were not reinferecitons)
den_reinfections_symp <- reinfections_symp[[2]] %>%
  mutate(n_Jan_Mar_2020=NA)

denominator <- rbind(covid_infection_symp[[2]],
                     first_infection_symp[[2]],
                     confirmed_infection_symp[[2]],
                     den_reinfections_symp ,
                     tested_negative_earliest_symp[[2]],
                     tested_negative_all_symp[[2]],   
                     PCR_negative_earliest_symp[[2]],
                     PCR_negative_all_symp[[2]]
)

# save exclusion table and denominator table
write.csv2(exclusion_table, here("results/exclusion_table.csv"))
write.csv2(denominator, here("results/denominator.csv"))


# generate any symptom cohorts 
covid_infection_any_symp          <- create_any_symptom(list_cohorts = covid_infection_symp)
first_infection_any_symp          <- create_any_symptom(list_cohorts = first_infection_symp)
confirmed_infection_any_symp      <- create_any_symptom(list_cohorts = confirmed_infection_symp)
reinfections_any_symp             <- create_any_symptom(list_cohorts = reinfections_symp)
tested_negative_earliest_any_symp <- create_any_symptom(list_cohorts = tested_negative_earliest_symp )
tested_negative_all_any_symp      <- create_any_symptom(list_cohorts = tested_negative_all_symp)
PCR_negative_earliest_any_symp    <- create_any_symptom(list_cohorts = PCR_negative_earliest_symp )
PCR_negative_all_any_symp         <- create_any_symptom(list_cohorts = PCR_negative_all_symp )

# we append any symptoms cohorts to the table
conn <- connect(connectionDetails)   

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
append_table(first_infection_any_symp)
append_table(confirmed_infection_any_symp)
append_table(reinfections_any_symp)
append_table(tested_negative_earliest_any_symp)
append_table(tested_negative_all_any_symp)
append_table(PCR_negative_all_any_symp)
append_table(PCR_negative_earliest_any_symp)

# generate cohorts inclding all symptoms per person&infection 

 
 
# cohort including all the symptoms reported per row
create_numb_symptom <- function(list_cohorts){
symps_cohort <- bind_rows(list_cohorts[1]) %>%
  mutate(main_cohort_id = substr(as.character(cohort_definition_id), 1,2)) %>%
  mutate(symptom_id = substr(as.character(cohort_definition_id), 3, 4)) %>%
  mutate(window_days = substr(as.character(cohort_definition_id), 5, 6)) %>%
  mutate(symptom_id = as.numeric(symptom_id)) %>%
  mutate(symptom =1) %>%  
  left_join(cohorts_ids %>% 
              select(cohort_definition_id, name_var) %>% 
              rename(symptom_id=cohort_definition_id)) %>%
  select(-cohort_end_date, -symptom_id, -cohort_definition_id) %>%
  group_by(subject_id, cohort_start_date, window_days) %>%
  spread(name_var, symptom) %>%
    ungroup()  %>%
 mutate_at(c(5:28), ~replace_na(.,0))
symps_cohort$all_symp<-rowSums(symps_cohort[,5:28],na.rm=T)
symps_cohort 
}

covid_infection_numb_symp          <- create_numb_symptom(list_cohorts = covid_infection_symp)
first_infection_numb_symp          <- create_numb_symptom(list_cohorts = first_infection_symp)
confirmed_infection_numb_symp      <- create_numb_symptom(list_cohorts = confirmed_infection_symp)
reinfections_numb_symp             <- create_numb_symptom(list_cohorts = reinfections_symp)
tested_negative_earliest_numb_symp <- create_numb_symptom(list_cohorts = tested_negative_earliest_symp )
tested_negative_all_numb_symp      <- create_numb_symptom(list_cohorts = tested_negative_all_symp)
PCR_negative_earliest_numb_symp    <- create_numb_symptom(list_cohorts = PCR_negative_earliest_symp )
PCR_negative_all_numb_symp         <- create_numb_symptom(list_cohorts = PCR_negative_all_symp )

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
                                                            
append_table_all(first_infection_numb_symp)
append_table_all(confirmed_infection_numb_symp)
append_table_all(reinfections_numb_symp)
append_table_all(tested_negative_earliest_numb_symp)
append_table_all(tested_negative_all_numb_symp)
append_table_all(PCR_negative_all_numb_symp)
append_table_all(PCR_negative_earliest_numb_symp)

# save the cohort for any symptoms keeping the date of start of symptoms as cohort_start_date 
# to use these cohorts with the IncidencePrevalence package
# these cohorts need a different id - 
# the symptom cohort id will be 89 (instead of 99)
change_any_symptom_date <- function(data){
  data <- data %>%
  mutate(cohort_start_date = cohort_end_date) %>%
  mutate(cohort_definition_id = (as.numeric(
                         substr(cohort_definition_id, 1, 2))*10^4 +
                         89*10^2+
                         as.numeric(substr(cohort_definition_id, 5,6)
                                    ))) %>%
  compute()
  data
}

covid_infection_any_symp          <- change_any_symptom_date(covid_infection_any_symp)
first_infection_any_symp          <- change_any_symptom_date(first_infection_any_symp)
confirmed_infection_any_symp      <- change_any_symptom_date(confirmed_infection_any_symp)
reinfections_any_symp             <- change_any_symptom_date(reinfections_any_symp)
tested_negative_earliest_any_symp <- change_any_symptom_date(tested_negative_earliest_any_symp)
tested_negative_all_any_symp      <- change_any_symptom_date(tested_negative_all_any_symp)
PCR_negative_earliest_any_symp    <- change_any_symptom_date(PCR_negative_earliest_any_symp)
PCR_negative_all_any_symp         <- change_any_symptom_date(PCR_negative_all_any_symp)
  
append_table(covid_infection_any_symp)
append_table(first_infection_any_symp)
append_table(confirmed_infection_any_symp)
append_table(reinfections_any_symp)
append_table(tested_negative_earliest_any_symp)
append_table(tested_negative_all_any_symp)
append_table(PCR_negative_all_any_symp)
append_table(PCR_negative_earliest_any_symp)


}

# Generate data for Descriptive Tables ----
if(generate_data_descriptive_table=="FALSE"){
print(paste0("- Skipping generating data for descriptive tables"))
} else { 
print(paste0("- Creating data for descriptive tables"))
  
# Generate cdm object to access the tables
cdm <- cdm_from_con(db, 
             cdm_schema = cdm_database_schema,
             write_schema = write_schema,
             cohort_tables = c("er_cohorts_for_longcov", # here are the vaccines talbes 
                               "er_long_covid_final_cohorts",  # here are the long covid cohorts
                               "er_long_covid_all_symptoms_cohorts")) # here are the cohorts with all symptoms

# get vaccines ids 
astrazeneca_id <- cohorts_ids %>%
                    filter(str_detect(name, "AstraZeneca")) %>% 
                    select(cohort_definition_id) %>% pull()
janssen_id <- cohorts_ids %>%
                    filter(str_detect(name, "Janssen")) %>% 
                    select(cohort_definition_id) %>% pull()
pfizer_id <- cohorts_ids %>%
                    filter(str_detect(name, "Pfizer")) %>% 
                    select(cohort_definition_id) %>% pull()
moderna_id <- cohorts_ids %>%
                    filter(str_detect(name, "Moderna")) %>% 
                    select(cohort_definition_id) %>% pull()


## cohrot of interest - first New Covid infections
# 90 days long Covid cohort
# first two digits are for the "mother cohort"
# digits 3,4 are for the symptoms cohort (99 is for any symptom") 
# last two digits for window period
## testing function
# main_cohort_interest <- cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == 22)
# long_covid_cohort <- cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == 229990)


# Function to get the data for the table 
generate_data_table1 <- function(main_cohort_interest, long_covid_cohort){
working_cohort <- main_cohort_interest %>% 
 rename(person_id=subject_id) %>%
  left_join(cdm$person  %>%
              select(person_id, gender_concept_id, year_of_birth) %>%
              mutate(gender= ifelse(gender_concept_id==8507, "Male",
                             ifelse(gender_concept_id==8532, "Female", NA ))) %>%
              select(-gender_concept_id)) %>%
  # SIDIAP specific  - MEDEA and nationality
 # left_join(cdm$observation %>%
 #             filter(observation_source_value == "medea") %>% 
 #             select(person_id, observation_source_value, value_as_string) %>%
 #             filter(value_as_string %in% c("R", "U1", "U2", "U3", "U4", "U5")) %>%
 #             mutate(medea=value_as_string) %>%
 #             select(person_id, medea)) %>%
 #   left_join(cdm$observation %>%
 #             select(person_id, observation_source_value, value_as_string) %>%
 #             filter(observation_source_value == "agr_nationality") %>%
 #            rename(nationality = value_as_string) %>%
 #             select(person_id, nationality))%>%
 #     # clean nationality
 #   mutate(nationality = ifelse(str_detect(nationality, "Austr"), "Australia & New Zealand", nationality)) %>%
 #   mutate(nationality_cat = ifelse( nationality== "Espanya", "Spain", 
 #                            ifelse(str_detect( nationality, "frica"),  "Africa",  
 #                            ifelse(str_detect( nationality, "rica central"), "Central & South America",
 #                            ifelse(str_detect( nationality, "del Nord"), "Europe & North America",    
 #                            ifelse(str_detect( nationality, "Europa"), "Europe & North America",       
 #                            ifelse(str_detect( nationality, "Europa oriental"),  "Eastern Europe",       
 #                            ifelse(str_detect( nationality, "del Sud"), "Central & South America",      
 #                            ifelse(str_detect( nationality, "Carib"), "Central & South America", 
 #                            ifelse(str_detect( nationality, "sia"), "Asia & Oceania",
 #                            ifelse(str_detect( nationality, "Melan"), "Asia & Oceania", 
 #                            ifelse(str_detect( nationality, "Polin"), "Asia & Oceania",
 #                            ifelse(str_detect( nationality, "Micro"), "Asia & Oceania",
 #                            ifelse(str_detect( nationality, "Ant"),"Asia & Oceania", 
 #                            ifelse(str_detect( nationality, "Australia & New Zealand"), "Asia & Oceania",
 #                                   NA))))))))))))))) %>%
 #   select(-nationality) %>%
  compute()
         
  # get age 
    working_cohort<- working_cohort %>%
    mutate(age= year(cohort_start_date)-year_of_birth) %>%
    select(-year_of_birth) %>%
      mutate(age_gr2=ifelse(age<=34,  "<=34",
                 ifelse(age>=35 & age<=49,  "35-49",    
                 ifelse(age>=50 & age<=64,  "50-64",    
                 ifelse(age>=65 & age<=79,  "65-79",    
                 ifelse(age>=80, ">=80",
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
   mutate(week_year = yearweek(as.Date(cohort_start_date))) %>%
   mutate(week_year = as.character(week_year)) %>%
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
 
 rm(autoimmune_disease.codes,
    asthma.codes,
    malignant_neoplastic_disease.codes,
    diabetes.codes,
    heart_disease.codes,
    hypertensive_disorder.codes,
    renal_impairment.codes,
    copd.codes,
    dementia.codes,
    autoimmune_disease,
    asthma,
    copd,
    dementia,
    diabetes,
    heart_disease,
    hypertensive_disorder,
    malignant_neoplastic_disease,
    renal_impairment)
 
 #!!! get vaccines ----
vaccines <- rbind(cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == astrazeneca_id) %>% mutate(vaccine_type = "ChAdOx1"),
                  cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == moderna_id) %>% mutate(vaccine_type = "mRNA-1273"),
                  cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == pfizer_id)  %>% mutate(vaccine_type = "BNT162b2"),
                  cdm$er_cohorts_for_longcov %>% filter(cohort_definition_id == janssen_id) %>% mutate(vaccine_type = "Ad26.COV2.S")
                  ) %>%
    select(-cohort_end_date, -cohort_definition_id) %>%
    full_join(main_cohort_interest) %>%
    filter(drug_exposure_start_date < cohort_start_date) %>%
    distinct() %>%
    compute()

 # SIDIAP specific vaccines   
# vaccinations <- cdm$drug_exposure%>% 
#       filter(drug_concept_id %in% 
#            c("37003436","724905",  "37003518", "739906")) %>% 
#   full_join(main_cohort_interest %>% rename(person_id=subject_id)) %>%
#   filter(drug_exposure_start_date < cohort_start_date) %>%
#   compute()  %>%
#    #"BNT162b2" #"ChAdOx1" #"mRNA-1273" #Ad26.COV2.S
#    select(person_id, drug_concept_id, drug_exposure_start_date) %>%
#   distinct() %>%
#    mutate(vaccine_type=
#             ifelse(drug_concept_id=="37003436", "BNT162b2",
#                    ifelse(drug_concept_id=="724905", "ChAdOx1",
#                           ifelse(drug_concept_id=="37003518", "mRNA-1273",
#                                  ifelse(drug_concept_id=="739906", "Ad26.COV2.S",NA ))))) %>%
#   compute()
 
# get  dates and vaccines types for the different doses
vaccines <- vaccinations %>%
  rename(person_id = subject_id,
         cohort_start_date = drug_exposure_start_date) %>%
      arrange(person_id,drug_exposure_start_date) %>% 
      group_by(person_id) %>% 
      mutate(seq=row_number())   %>%
      ungroup () %>%
      group_by(person_id) %>%
      arrange(person_id,drug_exposure_start_date) %>% 
  mutate(second_dose_type = ifelse(seq==1, 
                                   lead(vaccine_type, order_by = c("drug_exposure_start_date")), 
                                   NA),
             second_dose_date = ifelse(seq==1,
                                  lead(drug_exposure_start_date, order_by = c("drug_exposure_start_date")), 
                                  NA)) %>%
  ungroup() %>%
  compute()

vaccines <- vaccines %>%
  filter(seq!=2) %>%
  group_by(person_id) %>%
      arrange(person_id,drug_exposure_start_date) %>% 
  mutate( third_dose_type = ifelse(seq==1,
                                   lead(vaccine_type, order_by = c("drug_exposure_start_date")), 
                                   NA),
             third_dose_date = ifelse(seq==1,
                                  lead(drug_exposure_start_date, order_by = c("drug_exposure_start_date")),
                                  NA)) %>%
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

 ## we add long covid 90 days 
 working_cohort <- working_cohort %>%
   left_join( long_covid_cohort   %>% 
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
   select(-window_days, -cohort_id ) %>%
   rename(person_id = subject_id)) %>% 
    compute()
   
# add variables for timester of infection   
     working_cohort <- working_cohort %>%
     mutate(year = lubridate::year(cohort_start_date),
         month = lubridate::month(cohort_start_date)) %>%
  mutate(trimester=ifelse(year==2020 & month<4,                         "Jan-Mar 2020", 
                   ifelse(year==2020 & (month==4 | month==5| month==6), "Apr-Jun 2020", 
                   ifelse(year==2020 & (month==7 | month==8| month==9), "Jul-Sep 2020",
                   ifelse(year==2020 & month>9,                         "Oct-Dec 2020",
                   ifelse(year==2021 & month<4,                         "Jan-Mar 2021",
                   ifelse(year==2021 & (month==4 | month==5| month==6), "Apr-Jun 2021",
                   ifelse(year==2021 & (month==7 | month==8| month==9), "Jul-Sep 2021",
                   ifelse(year==2021 & month>9,                         "Oct-Dec 2021", 
                   NA))))))))) %>%
  select(-year, -month) %>%
   collect()%>%
       # add factors fo table 1 
     mutate(age_gr2= factor(age_gr2, 
                         levels = c("<=34",
                                    "35-49", 
                                    "50-64",
                                    "65-79",
                                    ">=80"))) %>%
    mutate(trimester= factor(trimester, 
                         levels = c("Jan-Mar 2020",
                                    "Apr-Jun 2020", 
                                    "Jul-Sep 2020",
                                    "Oct-Dec 2020",
                                    "Jan-Mar 2021",
                                    "Apr-Jun 2021",
                                    "Jul-Sep 2021",
                                    "Oct-Dec 2021"
                                    )))

 
working_cohort
}

# Symptoms - long Covid is 99
symptom_id <- 99

## New COVID-19 infection 
main_cohort_id <- cohorts_ids %>%
                    filter(str_detect(name, "New_Infection")) %>% 
                    select(cohort_definition_id) %>% pull()
long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id

table1_data_new_infections <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))

##First infection 
main_cohort_id <- 70
long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id

table1_data_first_infection <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))

## Confirmed infection 
main_cohort_id <- cohorts_ids %>%
                    filter(str_detect(name, "Tested_Positive")) %>% 
                    select(cohort_definition_id) %>% pull()
long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id

table1_data_confirmed_infection <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))

### Reinfections 
main_cohort_id <- 71
long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id

table1_data_reinfections <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))

## Tested negative, earliest 
main_cohort_id <- cohorts_ids %>%
                    filter(str_detect(name, "Tested_Negative_earliest")) %>% 
                    select(cohort_definition_id) %>% pull()
long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id


table1_data_tested_negative_earliest <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))

## Tested negative, all 
main_cohort_id <- cohorts_ids %>%
                    filter(str_detect(name, "Tested_Negative_all")) %>% 
                    select(cohort_definition_id) %>% pull()
long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id


table1_data_tested_negative_all <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))

## PCR negative, earliest 
main_cohort_id <- cohorts_ids %>%
                    filter(str_detect(name, "PCR_Negative_earliest")) %>% 
                    select(cohort_definition_id) %>% pull()
long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id


table1_data_pcr_negative_earliest <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))

## PCR negative, all 
main_cohort_id <- cohorts_ids %>%
                    filter(str_detect(name, "PCR_Negative_all")) %>% 
                    select(cohort_definition_id) %>% pull()
long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id


table1_data_pcr_negative_all <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))


## save results 
save(table1_data_new_infections,
     table1_data_first_infection,
     table1_data_confirmed_infection,
     table1_data_reinfections,
     table1_data_tested_negative_earliest,
     table1_data_tested_negative_all,
     table1_data_pcr_negative_all,
     table1_data_pcr_negative_earliest,
     file = here("data", "table1_data.Rdata"))



#### Generate  Matched cohort ----
#load(here("data", "table1_data.Rdata"))
rm(table1_data_pcr_negative_all, table1_data_pcr_negative_earliest, table1_data_tested_negative_all)

### For New covid infections, we'll include only people with a COVID diagnosis from September 2020 onward
table1_data_new_infections <- table1_data_new_infections %>% filter(cohort_start_date>=full_testing_start_date )
table1_data_first_infection <- table1_data_first_infection %>% filter(cohort_start_date>=full_testing_start_date )
table1_data_reinfections <- table1_data_reinfections%>% filter(cohort_start_date>=full_testing_start_date )


# Matching cohorts by week of test  and 5y age band
matching_cohorts <- function(cohort1, cohort2, 
                             name1, name2) {
  results_list <- list()
  data <- rbind(cohort1 %>%  mutate(cohort = 1, name = name1) ,
                cohort2 %>% mutate(cohort =0,   name = name2))  # comparator cohort
  # Matching
m.week_year <- matchit(cohort~  age_gr5+ gender+ week_year, 
                        data = data,
                        method = "nearest", 
                        exact = ~ age_gr5 + gender + week_year)

# extract results of matching
sum_matching <- summary(m.week_year)
# extract matched data
m.data <- match.data(
           # were matched data are
           object = m.week_year,
           # all for both controls and treated, can be treated or controls
           group = "all",
           # name for the weights - here all be 1
           weights = "ps_weights",
           # matched pair membership name
           subclass = "matched_pair",
           # dataset to add new columms
           data = data,
           drop.unmatched = TRUE)

# we also save this in survey object - this is needed to create later the summary tables using tableone anf tbl_summary
s.data <- svydesign(ids = ~matched_pair, # clusters
          data = m.data,
          weights= ~ps_weights) # weights

results_list <- list(sum_matching, m.data, s.data)
results_list
}

### New infections with tested negative  ----
m.new_covid_tested_negative_earliest <- matching_cohorts(
  cohort1 = table1_data_new_infections,
  name1 = "New infections",
  # to be consistent we  filter here on filtering date
  cohort2 = table1_data_tested_negative_earliest  %>% filter(cohort_start_date>=full_testing_start_date ), 
  name2 = "Tested negative, earliest"
)

### Confirmed infections with tested negative  ----
m.confirmed_infection_negative_earliest <- matching_cohorts(
  cohort1 = table1_data_confirmed_infection,
  name1 = "Confirmed infection",
  cohort2 = table1_data_tested_negative_earliest,
  name2 = "Tested negative, earliest",

)
### First infections with reinfections ---- 
m.first_infection_reinfections <- matching_cohorts(
  cohort1 = table1_data_first_infection,
  name1 = "First infection",
  # cohort2
  cohort2 = table1_data_reinfections,
  name2 = "Reinfections"
)

## save results 
save(m.new_covid_tested_negative_earliest,
     m.confirmed_infection_negative_earliest,
     m.first_infection_reinfections,
     file = here("data", "table1_data_matched.Rdata"))
}
  
# Generate html files  ----
if(generate_results  =="FALSE"){
print(paste0("Skipping  generating descriptive tables"))
} else { 
print(paste0("Generating descriptive tables"))
rmarkdown::render(here("Code/Comparing_symptoms.Rmd"))
rmarkdown::render(here("Code/Comparing_symptoms_matched.Rmd"))
  
} 


