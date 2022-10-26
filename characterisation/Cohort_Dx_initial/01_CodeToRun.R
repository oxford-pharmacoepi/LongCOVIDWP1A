
# install.packages("renv") # if not already installed, install renv from CRAN
# renv::activate()
# renv::restore() # this should prompt you to install the various packages required for the study


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

# database metadata and connection details -----

# database connection details
 server<-Sys.getenv("SERVER_JUN22")
 user<-Sys.getenv("DB_USER_JUN22")
 password<- Sys.getenv("DB_PASSWORD_JUN22")
 port<-Sys.getenv("DB_PORT_JUN22")
 host<-Sys.getenv("DB_HOST_JUN22")
# The name/ acronym for the database
server_dbi <-Sys.getenv("SERVER_DBI_JUN22")

# sql dialect used with the OHDSI SqlRender package
# schema that contains the OMOP CDM with patient-level data
# schema that contains the vocabularie
# schema where a results table will be created 

 targetDialect             <- "postgresql" 
 cdm_database_schema       <- "omop21t4_cmbd" 
 vocabulary_database_schema<- "omop21t4_cmbd"
 results_database_schema<- "results21t4_cmbd"




# driver for DatabaseConnector
downloadJdbcDrivers("postgresql", here()) # if you already have this you can omit and change pathToDriver below
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server =server,
                                             user = user,
                                             password = password,
                                             port = port ,
                                             pathToDriver = here())



db <- dbConnect(RPostgres::Postgres(),
                dbname = server_dbi,
                port = port,
                host = host, 
                user = user, 
                password = password)




# stem for tables to be created in your results schema for this analysis
# You can keep the above names or change them
# Note, any existing tables in your results schema with the same name will be overwritten
cohortTableStem<-"er_cohorts_for_longcov"

# Run analysis ----
source(here("Cohort_Dx_initial", "RunAnalysis.R"))

# Review results -----
# CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = here("Results"))
# CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = here("Results"))


