# Long Covid Characterisation study
# First check that the project "characterisation" is open

# # packages
library(here)
library(SqlRender)
library(DBI)
library(DatabaseConnector)
library(CohortGenerator)
library(tidyr)
library(dplyr)
library(dbplyr)
library(CDMConnector)
library(stringr)
library(lubridate)
library(MatchIt)
library(tsibble)
library(CirceR)
library(survey)
library(gtsummary)
library(tableone)
library(forcats)
library(epitools)
library(fmsb)
library(DescTools)
library(IncidencePrevalence)
library(DrugUtilisation)
# if (!require("pacman")) install.packages("pacman")
# library(pacman)
# p_load(SqlRender,
#        DBI,
#        DatabaseConnector,
#        CohortGenerator,
#        here,
#        tidyr,
#        dplyr,
#        dbplyr,
#        CDMConnector,
#        stringr,
#        lubridate,
#        MatchIt,
#        tsibble,
#        CirceR,
#        survey,
#        gtsummary,
#        tableone,
#        forcats,
#        fmsb,
#        DescTools,
#        epitools,
#        remotes)
# when getting error instlling from Github - 
# options(download.file.method = "libcurl")
# remotes::install_github("darwin-eu-dev/IncidencePrevalence")
# remotes::install_github("darwin-eu-dev/DrugUtilisation")

# Connection details - to change locally  ----

server    <- Sys.getenv("SERVER_Long_covid22t2")
server_dbi<- Sys.getenv("SERVER_DBI_Long_covid22t2")
user      <- Sys.getenv("DB_USER_Long_test")
password  <- Sys.getenv("DB_PASSWORD_Long_test")
port      <- Sys.getenv("DB_PORT_Long_test") 
host      <- Sys.getenv("DB_HOST_Long_test") 

targetDialect              <- "postgresql"
cdm_database_schema        <- "omop22t2_cmbd"
vocabulary_database_schema <- "omop21t2_cmbd" 
# schema to save results - this is some times called results_database_schema or similar
write_schema               <- "results22t2_cmbd"


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
# write_schema               <- "results21t2_test"


db <- dbConnect(RPostgres::Postgres(), 
                dbname = server_dbi,
                port = port,
                host = host, 
                user = user, 
                password = password)

## we also use connection details to insert later tables using OHDSI  tools
connectionDetails <- DatabaseConnector::downloadJdbcDrivers("postgresql", here::here())
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server =server,
                                                                user = user,
                                                                password = password,
                                                                port = port ,
                                                                pathToDriver = here::here())

## Parameters -----
# the name of the database to be displayed on reports - to change locally --
database_name <- "SIDIAP"

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

# Run analysis
source(here("Code/Run_Analysis.R"))

