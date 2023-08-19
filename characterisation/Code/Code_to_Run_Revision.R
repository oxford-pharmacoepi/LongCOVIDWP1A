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
library(DrugUtilisation)
library(PatientProfiles)
library(zip)

# Connection details - to change locally  ----

server_dbi <- "cdm_aurum_p21_000557"
user <- Sys.getenv("DB_USER")
password <- "martics33"#Sys.getenv("DB_PASSWORD")
port <- Sys.getenv("DB_PORT")
host <- "163.1.64.2"#Sys.getenv("DB_HOST")

db <- dbConnect(
  RPostgres::Postgres(),
  dbname = server_dbi,
  port = port,
  host = host,
  user = user,
  password = password
)

## Parameters -----
# the name of the database to be displayed on reports - to change locally --
database_name <- "CPRD AURUM"

covid_start_date    <- as.Date("2020-09-01", "%Y-%m-%d")
# censor when COVID-19 testing ends in SIDIAP - don't need to edit this
covid_end_date      <- as.Date("2022-03-28", "%Y-%m-%d")

cdm_database_schema <- "public"#"omop22t2_cmbd"
write_schema        <- "results"#"results22t2_cmbd"
write_stem          <- "lcwp1a_"

cdm <- cdmFromCon(
  con = db, 
  cdmSchema = cdm_database_schema, 
  writeSchema = c("schema" = write_schema, "prefix" = write_stem),
  cdmName = database_name
)

cdm$person %>%
  tally() %>%
  computeQuery()

# Run analysis
source(here("Code/Run_Analysis_Revision.R"))

