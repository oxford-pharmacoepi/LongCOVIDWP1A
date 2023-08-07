# packages
library(here)
library(DBI)
library(tidyr)
library(dplyr)
library(dbplyr)
library(CDMConnector)
library(stringr)
library(lubridate)
library(MatchIt)
library(PatientProfiles)
library(DrugUtilisation)
library(CodelistGenerator)

# Connection details - to change locally  ----
database_name <- "CPRD AURUM"
server_dbi <- "cdm_aurum_p21_000557"
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

# cohortStem
cohortStem <- "lcwp1_"

# Run analysis
source(here("Run_Analysis.R"))

