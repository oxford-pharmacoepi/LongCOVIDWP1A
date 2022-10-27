
# packages
library(SqlRender)
library(DBI)
library(DatabaseConnector)
library(here)
library(dplyr)
library(dbplyr)
library(tidyr)
library(CDMConnector)
library(stringr)


# Connection details ----
server    <- Sys.getenv("SERVER_JUN22")
server_dbi<- Sys.getenv("SERVER_DBI_JUN22")
user      <- Sys.getenv("DB_USER_JUN22")
password  <- Sys.getenv("DB_PASSWORD_JUN22")
port      <- Sys.getenv("DB_PORT_JUN22") 
host      <- Sys.getenv("DB_HOST_JUN22") 


db <- dbConnect(RPostgres::Postgres(), 
                dbname = server_dbi,
                port = port,
                host = host, 
                user = user, 
                password = password)

## we also cuse connection details to insert later tables using OHDSI  tools
connectionDetails <-DatabaseConnector::downloadJdbcDrivers("postgresql", here::here())
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server =server,
                                                                user = user,
                                                                password = password,
                                                                port = port ,
                                                                pathToDriver = here::here())
    
 
targetDialect              <- "postgresql"
cdm_database_schema        <- "omop21t4_cmbd" 
vocabulary_database_schema <- "omop21t4_cmbd" 
write_schema               <- "results21t4_cmbd"

# cdm object to access the tables easily
cdm <- cdm_from_con(db, 
             cdm_schema = cdm_database_schema,
             write_schema = write_schema,
             cohort_tables = c("er_cohorts_for_longcov"))

# create results folder if needed
cohorts.folder <- here::here("results")
if (!file.exists(cohorts.folder)){
  dir.create(cohorts.folder)
}

# parameters ----
# to get rid of covid diagnoses captured before the start of the pandemic --
# first case in Catalonia - 25 February 2020
covid_start_date    <- as.Date("2020-02-25", "%Y-%m-%d")
# we are not interested in old influenza diagnosis - so we can filter these
influenza_start_date<- as.Date("2019-12-01", "%Y-%m-%d")
# for symptoms, we only need symptoms occuring during covid + 180 days washout
symptoms_start_date <- as.Date("2019-08-29","%Y-%m-%d")
end_index_date      <- as.Date("2021-09-02", "%Y-%m-%d")

# Cohort ids -----
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

# get initial cohorts ----
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
                                influenza_start_date, 
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
#### remove prior tables fi needed
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
 mutate_at(c(5:27), ~replace_na(.,0))
symps_cohort$all_symp<-rowSums(symps_cohort[,5:27],na.rm=T)
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

covid_infection_any_symp          <- change_any_symptom_date( covid_infection_any_symp)
first_infection_any_symp          <- change_any_symptom_date( first_infection_any_symp)
confirmed_infection_any_symp      <- change_any_symptom_date( confirmed_infection_any_symp)
reinfections_any_symp             <- change_any_symptom_date( reinfections_any_symp)
tested_negative_earliest_any_symp <- change_any_symptom_date( tested_negative_earliest_any_symp)
tested_negative_all_any_symp      <- change_any_symptom_date( tested_negative_all_any_symp)
PCR_negative_earliest_any_symp    <- change_any_symptom_date( PCR_negative_earliest_any_symp)
PCR_negative_all_any_symp         <- change_any_symptom_date( PCR_negative_all_any_symp)
  
append_table(covid_infection_any_symp)
append_table(first_infection_any_symp)
append_table(confirmed_infection_any_symp)
append_table(reinfections_any_symp)
append_table(tested_negative_earliest_any_symp)
append_table(tested_negative_all_any_symp)
append_table(PCR_negative_all_any_symp)
append_table(PCR_negative_earliest_any_symp)


# hauria de tenir 2 cohorts (washout window) per cada simptoma i per cada covid base (4 cohorts) i control cohort (4)
# mes les cohrots de any symptom -- q de moment no estan a denominator
# pendent comprovar q no hi hagi alguna persona q ha tingut tots els simptomes en el periode de washout
## hi ha un problema amb la cohort de alergies, em surten 0 counts -- pendent d'arreglar
8*2*length(symptoms_ids)
denominator %>%select(cohort_id) %>% distinct() %>% nrow()

# 
# 
# ## falta tenir el dnominador de any symtpom - son tots excepte si alguna persona ha tingut tots els simptoes en el washout period-cosa q no sembla probable
# 
# ### explorar els numeros 
# # proporcio de long covid als 90 dies 
# n_long_cov <-rbind(
#   tibble(cohort_definition_id=  covid_infection %>% 
#            select(cohort_definition_id) %>% distinct() %>% pull(),
#          n= covid_infection_any_symp %>% 
#            filter(str_detect(as.character(cohort_definition_id), "90")) %>% 
#            nrow()),
#   tibble(cohort_definition_id= first_infection %>% 
#            select(cohort_definition_id) %>% distinct() %>% pull(),
#          n=  first_infection_any_symp %>% 
#            filter(str_detect(as.character(cohort_definition_id), "90")) %>%
#            nrow()),
#   tibble(cohort_definition_id = reinfections %>%
#             select(cohort_definition_id) %>% distinct() %>% pull(),
#          n= reinfections_any_symp %>% 
#            filter(str_detect(as.character(cohort_definition_id), "90")) %>%
#            nrow()),
#     tibble(cohort_definition_id = confirmed_infection %>%
#             select(cohort_definition_id) %>% distinct() %>% pull(),
#          n= confirmed_infection_any_symp %>% 
#            filter(str_detect(as.character(cohort_definition_id), "90")) %>%
#            nrow()),
#   tibble(cohort_definition_id = tested_negative_all[[1]]  %>%
#            select(cohort_definition_id) %>% distinct() %>% pull(),
#         n=  tested_negative_all_any_symp %>%
#            filter(str_detect(as.character(cohort_definition_id), "90")) %>%
#            nrow()),
#   tibble(cohort_definition_id= tested_negative_earliest[[1]]  %>% 
#            select(cohort_definition_id) %>% distinct() %>% pull(),
#          n=  tested_negative_earliest_any_symp %>% 
#            filter(str_detect(as.character(cohort_definition_id), "90")) %>%
#            nrow()),
#   tibble(cohort_definition_id = PCR_negative_earliest[[1]] %>%
#                      select(cohort_definition_id) %>% distinct() %>% pull(),
#         n=  PCR_negative_earliest_any_symp%>% 
#            filter(str_detect(as.character(cohort_definition_id), "90")) %>%
#            nrow()),
#   tibble(cohort_definition_id = PCR_negative_all[[1]]  %>%
#            select(cohort_definition_id) %>% distinct() %>% pull(),
#         n=  PCR_negative_all_any_symp %>%
#            filter(str_detect(as.character(cohort_definition_id), "90")) %>%
#            nrow()))
# 
# perc_long_cov <- exclusion_table %>%
#   filter(exclusion_reason== "Less than 120 days of follow-up" | 
#            name == 	"First COVID-19 infection" | 
#            name == "Reinfections") %>%
#   select(-exclusion_reason, -symptom)  %>%
#   left_join(n_long_cov) %>%
#   rename(N_any_symptom = n,
#          N_denominator = N_current) %>%
#   mutate(proportion = N_any_symptom/N_denominator *100) %>%
#   relocate(type, cohort_definition_id, name)
# 
# rm(n_long_cov)

# save stuff locally

# write.csv2(perc_long_cov, here("results/longCov90days.csv"))






















