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

#' Run the query to create all lookup table.
#'
#' This function will call and run the sql files to create the lookup tables. If parameter sqlFileList is left blank,
#' the default will call LocationTable.sql, CareSiteTable.sql, PersonTable.sql and ProviderTable.sql
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
#' @param sqlFileList               The list of the sql files for the lookup tables containing the sql to run.
#'                                     For example list("LocationTable.sql","PersonTable.sql")
#' @param oracleTempSchema             For Oracle only: the name of the database schema where you want
#'                                     all temporary tables to be managed. Requires create/insert
#'                                     permissions to this database.
#'
#' @export

createAllLookupTables <- function (connectionDetails,
                                  cdmDatabaseSchema,
                                  sourceDatabaseSchema,
                                  sqlFileList = NULL,
                                  oracleTempSchema = NULL){

  if (is.null(sqlFileList)) {
    sqlFileList <- list("LocationTable.sql", "PersonTable.sql", "CareSiteTable.sql", "ProviderTable.sql")
  }

  numSqlFiles <- length(sqlFileList)

  for(i in 1:numSqlFiles){

    createLookupTable(connectionDetails,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      sourceDatabaseSchema = sourceDatabaseSchema,
                      sqlFile = sqlFileList[i],
                      oracleTempSchema = oracleTempSchema)

  }


}


