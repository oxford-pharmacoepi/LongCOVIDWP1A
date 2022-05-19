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

cdmDatabaseSchema <- "insert-your-databaseSchema"
cohortDatabaseSchema <- "results"
cohortTable <- "longcovid"
databaseId <- "databasename"
vocabularyDatabaseSchema <- "insert-your-databaseSchema"


outputFolder <- "a location in R studio to hold results, example -- /home/kkostka/phenotypeLC-1X"
exportFolder <- outputFolder



baseUrl <- "https://atlas-demo.ohdsi.org/WebAPI"
# list of cohort ids
cohortIds <- c(1779164, 1779163, 1778828, 1778829, 1778986, 1778995, 1778994, 1778993, 1778992, 1778991, 1778990, 1778989, 1778988, 1778987, 1778985, 1778984, 1778983, 1778982, 1778981, 1778980, 1778979, 1778978, 1778976, 1778975, 1778974, 1778973, 1778972, 1778971, 1778970, 1778969, 1778968, 1778967, 1778966, 1778965, 1778964, 1778963, 1778962, 1778961, 1778960, 1778959, 1778958, 1778957, 1778956, 1778955, 1778954, 1778953, 1778940, 1778939, 1778952, 1778949, 1778945, 1778948, 1778944, 1778942, 1778941, 1778938, 1778937, 1778936, 1778935, 1778934, 1778933, 1778932, 1778831, 1778830, 1778827, 1778826, 1778825, 1778824, 1778823, 1778820, 1778819, 1778818, 1778815, 1778814, 1778813, 1778810, 1778809, 1778808, 1778804, 1778803, 1778802, 1778797, 1778796, 1778795, 1778792, 1778791, 1778789, 1778785, 1778784, 1778783, 1778781, 1778779, 1778780, 1778775, 1778774, 1778773, 1778772, 1778771, 1778770, 1778769, 1778768, 1778767, 1778766, 1778764, 1778763, 1778759, 1778758, 1778756, 1778755, 1778754, 1778753, 1778752, 1778751, 1778750, 1778749, 1778748, 1778747, 1778745, 1778742, 1778741, 1778740, 1778738, 1778739, 1778737, 1778736)
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


preMergeDiagnosticsFiles(exportFolder)
launchDiagnosticsExplorer(dataFolder = exportFolder, dataFile = "Premerged.RData")

