# Copyright 2017 Observational Health Data Sciences and Informatics
#
# This file is part of healthVerityBuildr
#
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

#' Chunk the raw data
#'
#' Take the first n digits of the person identifier to chunk the raw data
#' @param connectionDetails  An R object of type\cr\code{connectionDetails} created using the
#'                                     function \code{createConnectionDetails} in the
#'                                     \code{DatabaseConnector} package.
#' @param sourceDatabaseSchema  The name of the database schema that contains the raw data
#'                                     instance.  Requires read permissions to this database. On SQL
#'                                     Server, this should specifiy both the database and the schema,
#'                                     so for example 'raw_instance.dbo'.
#' @param oracleTempSchema  For Oracle only: the name of the database schema where you want
#'                                     all temporary tables to be managed. Requires create/insert
#'                                     permissions to this database.
#' @param nSubstring  The number of digits to substring the person identifier by. Default is 1.
#'
#' @export

chunkData <- function(connectionDetails,
                      sourceDatabaseSchema,
                      oracleTempSchema=NULL,
                      nSubstring=1){

  conn <- DatabaseConnector::connect(connectionDetails)

  sql <- paste0("SELECT DISTINCT @n_substring as nSubstring, SUBSTRING(HVID,1,@n_substring) as HVID_CHUNK
                FROM @source_schema.MEDICAL_CLAIMS
                UNION
                SELECT DISTINCT @n_substring as nSubstring, SUBSTRING(HVID,1,@n_substring) as HVID_CHUNK
                FROM @source_schema.PHARMACY_CLAIMS
                UNION
                SELECT DISTINCT @n_substring as nSubstring, SUBSTRING(HVID,1,@n_substring) as HVID_CHUNK
                FROM @source_schema.ENROLLMENT_COVERAGE
                ");
  sql <- SqlRender::renderSql(sql=sql,
                              source_schema = sourceDatabaseSchema,
                              n_substring = nSubstring)$sql
  sql <- SqlRender::translateSql(sql = sql,
                                 targetDialect = connectionDetails$dbms,
                                 oracleTempSchema = oracleTempSchema)$sql

  listHVIDChunks <- DatabaseConnector::querySql(conn=conn,
                                                sql)

  dbDisconnect(conn)

  return(listHVIDChunks)
}


