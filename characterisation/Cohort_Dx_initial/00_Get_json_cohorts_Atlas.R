#### warning #### 
#### this is not working for the cohorts that include Measurements (such as COVID one) with results in LOINC
# concept ids for LOINC are >10000000  (10^8) and numbers are rounded - so the concepts IDs end up being wrong!!
# the error applies also for allergy
# use this just for the Symptoms cohorts
library(here)
library(DatabaseConnector)
library(CohortDiagnostics)
library(ROhdsiWebApi)
# atlas server
baseUrl <- "https://atlas-demo.ohdsi.org/WebAPI"
# list of cohort ids
cohortIds <- c(
1780604,
1780604,
1780608,
1780612,
1780613,
1780614,
1780603,
1780615,
1780617,
1780618,
1780620,
1780621,
1780622,
1780619,
1780623,
1780611,
1780624,
1780625,
1780626,
1780627,
1780628,
1780629,
1781117,
1781135,
1780630,
1780846,
1780631,
1780632,
1780635,
1781126,
1780589
)

# cohorts with measurements


# create folder to save cohorts if needed
cohorts.folder <- here::here("Cohort_Dx_initial/1_InstantiateCohorts/Cohorts")
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
validJsonExpression <- RJSONIO::toJSON(cohort$expression,  digits = 23)
# save in jsons files with the cohort name
fileConn<-file(paste0(cohorts.folder,"/", name,".json"))
writeLines(validJsonExpression, fileConn)
close(fileConn)
}
