

# packages -----
# load the below packages 
# you should have them all available, with the required version, after
# having run renv::restore above
library(DatabaseConnector)
library(CohortDiagnostics)
library(CirceR)
library(CohortGenerator)
library(here)
library(stringr)
library(CDMConnector)



# database metadata and connection details -----

# database connection details
server<-Sys.getenv("SERVER_JUN22")
user<-Sys.getenv("DB_USER_JUN22")
password<- Sys.getenv("DB_PASSWORD_JUN22")
port<-Sys.getenv("DB_PORT_JUN22")
host<-Sys.getenv("DB_HOST_JUN22")
# # The name/ acronym for the database
 db.name<-Sys.getenv("SERVER_DBI_JUN22")

# sql dialect used with the OHDSI SqlRender package
# schema that contains the OMOP CDM with patient-level data
# schema that contains the vocabularie
# schema where a results table will be created 

targetDialect              <- "postgresql"
cdm_database_schema        <- "omop21t4_cmbd" 
vocabulary_database_schema <-"omop21t4_cmbd" 
write_schema               <-"results21t4_cmbd"

db <- dbConnect(RPostgres::Postgres(),
                dbname = server_dbi,
                port = port,
                host = host, 
                user = user, 
                password = password)

# Generate cdm object to access the tables
cdm <- cdm_from_con(db, 
             cdm_schema = cdm_database_schema,
             write_schema = write_schema)
# PCR (SARS-CoV-2) 586310 Genetic material using Molecular method
# antigen 37310257
#antibody 37310258
tests_ids <- c(586310 ,37310257)
tests <- cdm$measurement %>% 
  filter(measurement_concept_id %in% tests_ids) %>%
  compute()


tests %>% tally()
covid_start_date    <- as.Date("2021-12-25", "%Y-%m-%d")
tests <- tests %>%
  filter(measurement_date > covid_start_date) %>%
  collect()

# value_as_concept_id
# LOING negative: 45878583 - LA6577-6
# LOINC positive: 45884084
# LOINC undetermined: 45880649
# LOINC likely



# quiza el problema esta en los criterios de inclusion
# tenemos edad
cdm$person %>%
  select(year_of_birth) %>%
  collect() %>%
  ggplot(aes(x = year_of_birth)) +
  geom_histogram(bins = 30)

# pbservation period
cdm$observation_period %>%
  select(observation_period_start_date, observation_period_end_date) %>%
  mutate(observation_period = (observation_period_end_date - observation_period_start_date)/365, 25) %>%
  select(observation_period) %>%
  collect() %>%
  ggplot(aes(x = observation_period)) +
  geom_boxplot()
