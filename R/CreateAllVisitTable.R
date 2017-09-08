# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Run the query to create the all_visit table.
#'
#' This function will call and run the sql file to create the all_visit table and the assign_all_visit_ids table which are the first and second steps
#' to creating the visit_occurrence table.
#' Requires that the function \code{chunkData} is run first.
#' @param connectionDetails  An R object of type\cr\code{connectionDetails} created using the
#'                                     function \code{createConnectionDetails} in the
#'                                     \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema  The name of the database schema that contains the OMOP CDM
#'                                     instance.  Requires read permissions to this database. On SQL
#'                                     Server, this should specifiy both the database and the schema,
#'                                     so for example 'cdm_instance.dbo'.
#' @param sourceDatabaseSchema  The name of the database schema that contains the raw data
#'                                     instance.  Requires read permissions to this database. On SQL
#'                                     Server, this should specifiy both the database and the schema,
#'                                     so for example 'raw_instance.dbo'.
#' @param oracleTempSchema             For Oracle only: the name of the database schema where you want
#'                                     all temporary tables to be managed. Requires create/insert
#'                                     permissions to this database.
#' @param iteraterNum           The number of chunks of data the program should be looped over.
#'
#' @export

createAllVisitTable <- function(connectionDetails,
                                         cdmDatabaseSchema,
                                         sourceDatabaseSchema,
                                         oracleTempSchema = NULL,
                                         iteraterNum){

  if(is.null(listHVIDChunks)){
    return("Please run the chunkData function first to create the data chunks")
  } else {

    conn <- connect(connectionDetails)
    tempTableNames <- paste0("#temp_all_visits_",1:iteraterNum)
    tableNamescl <- paste0(cdmDatabaseSchema,".CLAIM_LINES_",1:iteraterNum)
    sqlFile <- "AllVisitTable.sql"

    #STEP 1
    for(i in 1:iteraterNum){
      print(paste0(i,":",iteraterNum," run All_Visits table (part 1 of visit_occurrence) for HVIDs starting with ", listHVIDChunks$HVID_CHUNK[i]))
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFile,
                                               packageName = "healthVerityBuildr",
                                               dbms = connectionDetails$dbms,
                                               oracleTempSchema = oracleTempSchema,
                                               result_claim_lines = tableNamescl[i],
                                               result_temp_all_visits = tempTableNames[i],
                                               source_schema = sourceDatabaseSchema,
                                               HVID_CHUNK = listHVIDChunks$HVID_CHUNK[i],
                                               n_substring = listHVIDChunks$NSUBSTRING[i])

      DatabaseConnector::executeSql(conn, sql)
    }

    sql <- paste0("IF OBJECT_ID('@cdm_schema.ALL_VISITS', 'U') IS NOT NULL DROP TABLE @cdm_schema.ALL_VISITS\n
              CREATE TABLE @cdm_schema.ALL_VISITS WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH(HVID)) AS\n
                  SELECT *, ROW_NUMBER () OVER (ORDER BY (SELECT(NULL))) AS VISIT_OCCURRENCE_ID \nFROM (\n",
                  paste0(paste0("  SELECT * FROM ", tempTableNames), collapse = "\n  UNION\n"),
                  "\n) temp")
    sql <- SqlRender::renderSql(sql = sql,
                                cdm_schema = cdmDatabaseSchema)$sql
    sql <- SqlRender::translateSql(sql = sql,
                                   targetDialect = connectionDetails$dbms)$sql

    writeLines("Stack all_visit temp tables on top of each other")
    DatabaseConnector::executeSql(conn, sql, progressBar = TRUE, reportOverallTime = TRUE)

    #STEP 2
    tempTableNames <- paste0("#temp_aavi_",1:iteraterNum)
    sqlFile <- "AAVITable.sql"

    for(i in 1:iteraterNum){
      print(paste0(i,":",iteraterNum," run Assign_All_Visit_IDs table (part 2 of visit_occurrence) for HVIDs starting with ", listHVIDChunks$HVID_CHUNK[i]))
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFile,
                                               packageName = "healthVerityBuildr",
                                               dbms = connectionDetails$dbms,
                                               oracleTempSchema = oracleTempSchema,
                                               result_claim_lines = tableNamescl[i],
                                               result_temp_aavi = tempTableNames[i],
                                               cdm_schema = cdmDatabaseSchema,
                                               HVID_CHUNK = listHVIDChunks$HVID_CHUNK[i],
                                               n_substring = listHVIDChunks$NSUBSTRING[i])

      DatabaseConnector::executeSql(conn, sql)
    }

    sql <- paste0("IF OBJECT_ID('@cdm_schema.ASSIGN_ALL_VISIT_IDS', 'U') IS NOT NULL DROP TABLE @cdm_schema.ASSIGN_ALL_VISIT_IDS\n
                  CREATE TABLE @cdm_schema.ASSIGN_ALL_VISIT_IDS WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH(STEM_ID)) AS\n
                  SELECT * \nFROM (\n",
                  paste0(paste0("  SELECT * FROM ", tempTableNames), collapse = "\n  UNION ALL\n"),
                  "\n) temp")
    sql <- SqlRender::renderSql(sql = sql,
                                cdm_schema = cdmDatabaseSchema)$sql
    sql <- SqlRender::translateSql(sql = sql,
                                   targetDialect = connectionDetails$dbms)$sql

    writeLines("Stack assign_all_visit_id temp tables on top of each other")
    DatabaseConnector::executeSql(conn, sql, progressBar = TRUE, reportOverallTime = TRUE)

    dbDisconnect(conn)
  }
}
