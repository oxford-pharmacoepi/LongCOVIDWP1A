
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
# when getting error instlling from Github - 
# options(download.file.method = "libcurl")
#remotes::install_github("darwin-eu/IncidencePrevalence@issue_189")
 remotes::install_github("darwin-eu/IncidencePrevalence")
library(IncidencePrevalence)


# Connection details ----

server    <- Sys.getenv("SERVER_Long_covid22t2")
server_dbi<- Sys.getenv("SERVER_DBI_Long_covid22t2")
user      <- Sys.getenv("DB_USER_Long_test")
password  <- Sys.getenv("DB_PASSWORD_Long_test")
port      <- Sys.getenv("DB_PORT_Long_test") 
host      <- Sys.getenv("DB_HOST_Long_test") 

targetDialect              <- "postgresql"
cdm_database_schema        <- "omop22t2_cmbd"
vocabulary_database_schema <- "omop21t2_cmbd" 
# schema to save results
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

## we also cuse connection details to insert later tables using OHDSI  tools
connectionDetails <- DatabaseConnector::downloadJdbcDrivers("postgresql", here::here())
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server =server,
                                                                user = user,
                                                                password = password,
                                                                port = port ,
                                                                pathToDriver = here::here())

## Parameters ----
# database name for reports
database_name <- "SIDIAP"
#  we'll keep only records after the first wave  --
covid_start_date    <- as.Date("2020-09-01", "%Y-%m-%d")
# censor when COVID-19 testing ends - country specific
covid_end_date      <- as.Date("2022-03-28", "%Y-%m-%d")


# Create initial Json cohorts ---- 
create_initial_json_cohorts     <- FALSE
# Create Long Covid  cohorts and save in the db
create_long_covid_cohorts       <- FALSE
# Generate data for descritptive tables
generate_data_baseline_char     <- FALSE
match_cohorts                   <- FALSE
generate_results                <- TRUE
get_incidence_rate              <- TRUE

# Run analysis
source(here("Code/Run_Analysis.R"))

