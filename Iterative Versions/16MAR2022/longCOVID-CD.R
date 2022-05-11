###################################################################
connectionDetails <- createConnectionDetails(dbms="postgresql",
                                             server="",
                                             user="",
                                             password="",
                                             port="5432",
                                             pathToDriver = file.path("~/jdbcDrivers"))

####################################################################


Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "~/jdbcDrivers")
library(DatabaseConnector)
library(CohortDiagnostics)

cdmDatabaseSchema <- "public"
cohortDatabaseSchema <- "results"
cohortTable <- "longcovid"
databaseId <- "p20_059_cdm_aurum"
vocabularyDatabaseSchema <- "p20_059_cdm_aurum"


outputFolder <- "/home/kkostka/phenotypeLCv2"
exportFolder <- outputFolder



baseUrl <- "https://atlas-demo.ohdsi.org/WebAPI"
# list of cohort ids
cohortIds <- c(1778725, 1778724, 1778723, 1778722, 1778721, 1778720, 1778718, 1778719, 1778717, 1778716, 1778715, 1778714, 1778711, 1778713, 1778712, 1778710, 1778709, 1778708, 1778707, 1778706, 1778705, 1778704, 1778701, 1778700, 1778703, 1778702, 1778699, 1778698, 1778697, 1778696, 1778695, 1778694, 1778689, 1778688, 1778687, 1778686, 1778685, 1778684, 1778683, 1778682, 1778681, 1778680, 1778679, 1778678, 1778677, 1778676, 1778675, 1778674, 1778672, 1778673, 1778671, 1778670, 1778669, 1778665, 1778664, 1778666, 1778667, 1778668, 1778662, 1778663, 1778661, 1778660, 1778659)

cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(baseUrl = baseUrl,
                                                               cohortIds = cohortIds,
                                                               generateStats = TRUE)

cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)

CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                    cohortTableNames = cohortTableNames,
                                    cohortDatabaseSchema = "results",
                                    incremental = FALSE)


CohortGenerator::generateCohortSet(connectionDetails= connectionDetails,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTableNames = cohortTableNames,
                                   cohortDefinitionSet = cohortDefinitionSet,
                                   incremental = FALSE)


executeDiagnostics(cohortDefinitionSet,
                   connectionDetails = connectionDetails,
                   cohortTable = cohortTable,
                   cohortDatabaseSchema = cohortDatabaseSchema,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   exportFolder = exportFolder,
                   databaseId = "p20_059_cdm_aurum",
                   minCellCount = 5)


#NOTE: NEED TO RERUN WITH 1778658#

preMergeDiagnosticsFiles(exportFolder)
launchDiagnosticsExplorer(dataFolder = exportFolder, dataFile = "Premerged.RData")
