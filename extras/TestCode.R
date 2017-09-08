library(healthVerityBuildr)

connectionDetails <- createConnectionDetails(dbms = ,
                                             server = ,
                                             port =

)

cdmDatabaseSchema <-
sourceDatabaseSchema <-
vocabDatabaseSchema <-
oracleTempSchema <- NULL

listHVIDChunks <- chunkData(connectionDetails = connectionDetails,
                  sourceDatabaseSchema = sourceDatabaseSchema,
                  oracleTempSchema = oracleTempSchema,
                  nSubstring = 1)

iteraterNum <- getIteraterNum()

createVocabTables(connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  vocabDatabaseSchema = vocabDatabaseSchema,
                  oracleTempSchema = oracleTempSchema)

createAllLookupTables(connectionDetails = connectionDetails,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      sourceDatabaseSchema = sourceDatabaseSchema,
                      sqlFileList = NULL,
                      oracleTempSchema = oracleTempSchema)

createObservationPeriodTable(connectionDetails = connectionDetails,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             sourceDatabaseSchema = sourceDatabaseSchema,
                             oracleTempSchema = oracleTempSchema,
                             createWebmdObsPeriod = TRUE,
                             iteraterNum = iteraterNum)

createPayerPlanPeriodTable(connectionDetails = connectionDetails,
                           cdmDatabaseSchema = cdmDatabaseSchema,
                           sourceDatabaseSchema = sourceDatabaseSchema,
                           oracleTempSchema = oracleTempSchema,
                           iteraterNum = iteraterNum)

createVisitOccurrenceTable(connectionDetails = connectionDetails,
                           cdmDatabaseSchema = cdmDatabaseSchema,
                           sourceDatabaseSchema = sourceDatabaseSchema,
                           oracleTempSchema = oracleTempSchema,
                           runAllParts = TRUE,
                           iteraterNum = iteraterNum)

createSTEMTable(connectionDetails = connectionDetails,
                cdmDatabaseSchema = cdmDatabaseSchema,
                sourceDatabaseSchema = sourceDatabaseSchema,
                oracleTempSchema = oracleTempSchema,
                iteraterNum = iteraterNum)

# createEventTables(connectionDetails = connectionDetails,
#                   cdmDatabaseSchema = cdmDatabaseSchema,
#                   oracleTempSchema = oracleTempSchema)

createConditionOccurrenceTable(connectionDetails = connectionDetails,
                               cdmDatabaseSchema = cdmDatabaseSchema
                                )
