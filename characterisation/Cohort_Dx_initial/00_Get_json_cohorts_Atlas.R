#### warning #### 
#### this is not working for the cohorts that include Measurements (such as COVID one) with results in LOINC
# concept ids for LOINC are >10000000  (10^8) and numbers are rounded - so the concepts IDs end up being wrong!!
# use this just for the Symptoms cohorts
library(here)
library(DatabaseConnector)
library(CohortDiagnostics)
library(ROhdsiWebApi)
# atlas server
baseUrl <- "https://atlas-demo.ohdsi.org/WebAPI"
# list of cohort ids
cohortIds <- c(
1780619,1780589,1780628,1780624,1780622,1780617,1780615,1780618,1780621,1780620,
  1780626,1780612,1780625,1780614,1780613,1780631,1780623,1780627,1780629,1780630,
1780632,1780603,1780604,1780608
)

# cohorts with measurements
cohorts_measurements <- c(1780611,1780580,1780634,1780587,1780635,1780582,1780581)


# create folder to save cohorts if needed
cohorts.folder <- here::here("1_InstantiateCohorts/Cohorts_v2")
if (!file.exists(cohorts.folder)){
  dir.create(cohorts.folder)
}

for (i in 1:length(cohortIds)){
cohortId <- cohortIds[i]
 message(paste0("working on ",cohortId))
 # get the cohort definition
cohort  <- getCohortDefinition(baseUrl = baseUrl, cohortId = cohortId)
# get cohort name
name <- cohort$name
# get json expression
validJsonExpression <- RJSONIO::toJSON(cohort$expression)
# save in jsons files with the cohort name
fileConn<-file(paste0(cohorts.folder,"/", name,".json"))
writeLines(validJsonExpression, fileConn)
close(fileConn)
}
