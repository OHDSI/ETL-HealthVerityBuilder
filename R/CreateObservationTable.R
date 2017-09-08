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

#' Run the query to create the observation table.
#'
#' This function will call and run the sql file to create the observation table from the STEM table.
#' Requires that the function \code{chunkData} is run first.
#' @param connectionDetails  An R object of type\cr\code{connectionDetails} created using the
#'                                     function \code{createConnectionDetails} in the
#'                                     \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema  The name of the database schema that contains the OMOP CDM
#'                                     instance.  Requires read permissions to this database. On SQL
#'                                     Server, this should specifiy both the database and the schema,
#'                                     so for example 'cdm_instance.dbo'.
#' @param oracleTempSchema             For Oracle only: the name of the database schema where you want
#'                                     all temporary tables to be managed. Requires create/insert
#'                                     permissions to this database.
#'
#' @export

createObservationTable <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    oracleTempSchema = NULL){

  conn <- connect(connectionDetails)
  sqlFile <- "ObservationTable.sql"

  print(paste0("Run observation table"))
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFile,
                                           packageName = "healthVerityBuildr",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_schema = cdmDatabaseSchema)

  DatabaseConnector::executeSql(conn, sql, progressBar = TRUE, reportOverallTime = TRUE)
  dbDisconnect(conn)

}
