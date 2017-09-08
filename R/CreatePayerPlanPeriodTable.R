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

#' Run the query to create the payer_plan_period table.
#'
#' This function will call and run the sql file to create the payer_plan_period table. Requires that the function \code{chunkData} is run first.
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

createPayerPlanPeriodTable <- function(connectionDetails,
                                         cdmDatabaseSchema,
                                         sourceDatabaseSchema,
                                         oracleTempSchema = NULL,
                                         iteraterNum){

  if(is.null(listHVIDChunks)){
    return("Please run the chunkData function first to create the data chunks")
  } else {

    conn <- connect(connectionDetails)
    tempTableNames <- paste0("#temp_pp_period_",1:iteraterNum)
    sqlFile <- "PayerPlanPeriodTable.sql"

    for(i in 1:iteraterNum){
      print(paste0(i,":",iteraterNum," run Payer_Plan_Period for HVIDs starting with ", listHVIDChunks$HVID_CHUNK[i]))
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFile,
                                               packageName = "healthVerityBuildr",
                                               dbms = connectionDetails$dbms,
                                               oracleTempSchema = oracleTempSchema,
                                               result_temp_table = tempTableNames[i],
                                               cdm_schema = cdmDatabaseSchema,
                                               source_schema = sourceDatabaseSchema,
                                               HVID_CHUNK = listHVIDChunks$HVID_CHUNK[i],
                                               n_substring = listHVIDChunks$NSUBSTRING[i])

      DatabaseConnector::executeSql(conn, sql)
    }

    sql <- paste0("IF OBJECT_ID('@cdm_schema.PAYER_PLAN_PERIOD') IS NOT NULL DROP TABLE @cdm_schema.[PAYER_PLAN_PERIOD]\n
                CREATE TABLE @cdm_schema.[PAYER_PLAN_PERIOD]\n
              WITH (\n
              CLUSTERED COLUMNSTORE INDEX,\n
              DISTRIBUTION = HASH(PERSON_ID)\n
              ) AS\n
              SELECT ROW_NUMBER () OVER (ORDER BY (SELECT NULL)) AS PAYER_PLAN_PERIOD_ID, * \nFROM (\n",
                  paste0(paste0("  SELECT * FROM ", tempTableNames), collapse = "\n  UNION ALL\n"),
                  "\n) temp")
    sql <- SqlRender::renderSql(sql = sql,
                                cdm_schema = cdmDatabaseSchema)$sql
    sql <- SqlRender::translateSql(sql = sql,
                                   targetDialect = connectionDetails$dbms)$sql

    writeLines("Stack payer_plan_period temp tables on top of each other")
    DatabaseConnector::executeSql(conn, sql, progressBar = TRUE, reportOverallTime = TRUE)

  }
  dbDisconnect(conn)
}
