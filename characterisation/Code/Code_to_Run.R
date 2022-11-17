
# packages
if (!require("pacman")) install.packages("pacman")
library(pacman)

p_load(SqlRender,
       DBI,
       DatabaseConnector,
       CohortGenerator,
       CohortDiagnostics,
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
       survey)

# Connection details ----
server    <- Sys.getenv("SERVER_JUN22")
server_dbi<- Sys.getenv("SERVER_DBI_JUN22")
user      <- Sys.getenv("DB_USER_JUN22")
password  <- Sys.getenv("DB_PASSWORD_JUN22")
port      <- Sys.getenv("DB_PORT_JUN22") 
host      <- Sys.getenv("DB_HOST_JUN22") 

targetDialect              <- "postgresql"
cdm_database_schema        <- "omop21t4_cmbd" 
vocabulary_database_schema <- "omop21t4_cmbd" 
# this is the schema to save results in the database
write_schema               <- "results21t4_cmbd"


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
# to get rid of covid diagnoses captured before the start of the pandemic --
covid_start_date    <- as.Date("2020-02-25", "%Y-%m-%d")
# we are not interested in old influenza diagnoses - so we can filter these
influenza_start_date<- as.Date("2019-12-01", "%Y-%m-%d")
# for symptoms, we only need symptoms occurring during covid + 180 days washout
symptoms_start_date <- as.Date("2019-08-29","%Y-%m-%d")
end_index_date      <- as.Date("2021-09-02", "%Y-%m-%d")
# for filtering some results 
full_testing_start_date <- as.Date("2020-09-01", "%Y-%m-%d")

# Create initial Json cohorts ---- 
create_initial_json_cohorts     <- TRUE
# Crate Long Covid  cohorts and save in the db
create_long_covid_cohorts       <- TRUE
# Generate data for descritpive tables and save locally
generate_data_descriptive_table <- TRUE
# Generate Html with results files
generate_results                <- TRUE


# Window for long COvid - 90 or 28
window_id <- 90

# Run analysis
source(here("Code/Run_Analysis.R"))


# Results script
