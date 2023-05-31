# Long Covid Characterisation study
# First check that the project "characterisation" is open

# # packages
library(here)
library(DBI)
library(tidyr)
library(dplyr)
library(dbplyr)
library(CDMConnector)
library(stringr)
library(lubridate)
library(MatchIt)
library(tsibble)
library(survey)
library(gtsummary)
library(tableone)
library(forcats)
library(epitools)
library(fmsb)
library(DescTools)
library(IncidencePrevalence)
library(PatientProfiles)

# Connection details - to change locally  ----
database_name <- "CPRD AURUM"
server_dbi<- Sys.getenv("DB_SERVER_p20_059_cdm_aurum_dbi")
user      <- Sys.getenv("DB_USER")
password  <- Sys.getenv("DB_PASSWORD")
port      <- Sys.getenv("DB_PORT") 
host      <- Sys.getenv("DB_HOST") 

cdm_database_schema        <- "public"
write_schema               <- "results"

db <- dbConnect(RPostgres::Postgres(), 
                dbname = server_dbi,
                port = port,
                host = host, 
                user = user, 
                password = password)

# create cdm object
cdm <- cdmFromCon(
  con = db, 
  cdmSchema = cdm_database_schema, 
  writeSchema = write_schema,
  cdmName = database_name
)

covid_start_date    <- as.Date("2020-09-01", "%Y-%m-%d")
# censor when COVID-19 testing ends in SIDIAP - don't need to edit this
covid_end_date      <- as.Date("2022-03-28", "%Y-%m-%d")

## Code steps -- all set to True for the first run
create_initial_json_cohorts     <- TRUE
create_long_covid_cohorts       <- TRUE
generate_data_baseline_char     <- TRUE
match_cohorts                   <- TRUE
generate_results                <- TRUE
get_incidence_rate              <- TRUE

# cohortStem
cohortStem <- "longcovid_wp1"

# Run analysis
source(here("Code/Run_Analysis.R"))

