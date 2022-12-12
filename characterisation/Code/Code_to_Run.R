
# packages
if (!require("pacman")) install.packages("pacman")
library(pacman)

p_load(SqlRender,
       DBI,
       DatabaseConnector,
       CohortGenerator,
       here,
       tidyr,
       dplyr,
       dbplyr,
       CDMConnector,
       stringr,
       lubridate,
       MatchIt,
       tsibble,
       CirceR,
       survey,
       gtsummary,
       tableone,
       forcats,
       survey,
       fmsb,
       DescTools,
       remotes)
#remotes::install_github("darwin-eu/IncidencePrevalence@issue_189")
# remotes::install_github("darwin-eu/IncidencePrevalence")
library(IncidencePrevalence)

# Connection details ----


# server    <- Sys.getenv("SERVER_JUN22")
# server_dbi<- Sys.getenv("SERVER_DBI_JUN22")
# user      <- Sys.getenv("DB_USER_JUN22")
# password  <- Sys.getenv("DB_PASSWORD_JUN22")
# port      <- Sys.getenv("DB_PORT_JUN22") 
# host      <- Sys.getenv("DB_HOST_JUN22") 
# targetDialect              <- "postgresql"
# cdm_database_schema        <- "omop21t4_cmbd" 
# vocabulary_database_schema <- "omop21t4_cmbd" 
# write_schema               <- "results21t4_cmbd"


server    <- Sys.getenv("SERVER_Long_test")
server_dbi<- Sys.getenv("SERVER_DBI_Long_test")
user      <- Sys.getenv("DB_USER_Long_test")
password  <- Sys.getenv("DB_PASSWORD_Long_test")
port      <- Sys.getenv("DB_PORT_Long_test") 
host      <- Sys.getenv("DB_HOST_Long_test") 

targetDialect              <- "postgresql"
cdm_database_schema        <- "omop21t2_test" 
vocabulary_database_schema <- "omop21t2_test" 

# this is the schema to save results in the database
write_schema               <- "results21t2_test"


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

## Parameters ----
# database name for reports
database_name <- "SIDIAP"
#  records after the first wave  --
covid_start_date    <- as.Date("2020-09-01", "%Y-%m-%d")
# 120 d prior to end of observation - this is needed for the IncidencePrevalence
study_end_date      <- as.Date("2021-02-28", "%Y-%m-%d")
# censor when COVID-19 testing ends - country specific
# pensar en aixo - pq aleshores tambe hauria de filtar pq no entri gent diagnosticada despres d'axi
covid_end_date      <- as.Date("2022-03-31", "%Y-%m-%d")


# Create initial Json cohorts ---- 
create_initial_json_cohorts     <- FALSE
# Crate Long Covid  cohorts and save in the db
create_long_covid_cohorts       <- FALSE
# Generate data for descritptive tables
generate_data_baseline_char     <- FALSE
match_cohorts                   <- FALSE
# Generate results 
generate_results                <- FALSE
get_incidence_rate              <- TRUE

# Run analysis
source(here("Code/Run_Analysis.R"))

