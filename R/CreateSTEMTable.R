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

#' Run the query to create the STEM table.
#'
#' This function will call and run the sql files to create the STEM table.
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

createSTEMTable <- function(connectionDetails,
                                       cdmDatabaseSchema,
                                       sourceDatabaseSchema,
                                       oracleTempSchema = NULL,
                                       runAllParts = TRUE,
                                       iteraterNum){

  if(is.null(listHVIDChunks)){
    return("Please run the chunkData function first to create the data chunks")
  } else {

    conn <- connect(connectionDetails)
    tempTableNames41 <- paste0("#temp_41_",1:iteraterNum)
    tempTableNames421 <- paste0("#temp_421_",1:iteraterNum)
    tempTableNames422 <- paste0("#temp_422_",1:iteraterNum)
    tempTableNames43 <- paste0("#temp_43_",1:iteraterNum)

    sqlFile <- "STEMTable1.sql"

    for(i in 1:iteraterNum){
      print(paste0(i,":",iteraterNum," run STEM table part 1 for HVIDs starting with ", listHVIDChunks$HVID_CHUNK[i]))
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFile,
                                               packageName = "healthVerityBuildr",
                                               dbms = connectionDetails$dbms,
                                               oracleTempSchema = oracleTempSchema,
                                               cdm_schema = cdmDatabaseSchema,
                                               source_schema = sourceDatabaseSchema,
                                               HVID_CHUNK = listHVIDChunks$HVID_CHUNK[i],
                                               n_substring = listHVIDChunks$NSUBSTRING[i],
                                               result_temp_table = tempTableNames41[i])

      DatabaseConnector::executeSql(conn, sql)
    }

    sqlFile <- "STEMTable21.sql"

    for(i in 1:iteraterNum){
      print(paste0(i,":",iteraterNum," run STEM table part 2.1 for HVIDs starting with ", listHVIDChunks$HVID_CHUNK[i]))
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFile,
                                               packageName = "healthVerityBuildr",
                                               dbms = connectionDetails$dbms,
                                               oracleTempSchema = oracleTempSchema,
                                               cdm_schema = cdmDatabaseSchema,
                                               source_schema = sourceDatabaseSchema,
                                               HVID_CHUNK = listHVIDChunks$HVID_CHUNK[i],
                                               n_substring = listHVIDChunks$NSUBSTRING[i],
                                               result_temp_table = tempTableNames421[i])

      DatabaseConnector::executeSql(conn, sql)
    }

    sqlFile <- "STEMTable22.sql"

    for(i in 1:iteraterNum){
      print(paste0(i,":",iteraterNum," run STEM table part 2.2 for HVIDs starting with ", listHVIDChunks$HVID_CHUNK[i]))
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFile,
                                               packageName = "healthVerityBuildr",
                                               dbms = connectionDetails$dbms,
                                               oracleTempSchema = oracleTempSchema,
                                               cdm_schema = cdmDatabaseSchema,
                                               source_schema = sourceDatabaseSchema,
                                               HVID_CHUNK = listHVIDChunks$HVID_CHUNK[i],
                                               n_substring = listHVIDChunks$NSUBSTRING[i],
                                               result_temp_table = tempTableNames422[i])

      DatabaseConnector::executeSql(conn, sql)
    }

    sqlFile <- "STEMTable3.sql"

    for(i in 1:iteraterNum){
      print(paste0(i,":",iteraterNum," run STEM table part 3 for HVIDs starting with ", listHVIDChunks$HVID_CHUNK[i]))
      sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFile,
                                               packageName = "healthVerityBuildr",
                                               dbms = connectionDetails$dbms,
                                               oracleTempSchema = oracleTempSchema,
                                               cdm_schema = cdmDatabaseSchema,
                                               source_schema = sourceDatabaseSchema,
                                               HVID_CHUNK = listHVIDChunks$HVID_CHUNK[i],
                                               n_substring = listHVIDChunks$NSUBSTRING[i],
                                               result_temp_table = tempTableNames43[i])

      DatabaseConnector::executeSql(conn, sql)
    }

    sqlFile <- "inst/sql/sql_server/STEMTable4.sql"
    writeLines("Stack temp tables on top of each other")
    createTableSql <- SqlRender::readSql(sqlFile)
    sql <- paste0(createTableSql,
                  paste0(paste0("  SELECT * FROM ", tempTableNames41),collapse = "\n  UNION ALL\n"),
                  paste0("\n  UNION ALL\n"),
                  paste0(paste0("  SELECT * FROM ", tempTableNames421),collapse = "\n  UNION ALL\n"),
                  paste0("\n  UNION ALL\n"),
                  paste0(paste0("  SELECT * FROM ", tempTableNames422),collapse = "\n  UNION ALL\n"),
                  paste0("\n  UNION ALL\n"),
                  paste0(paste0("  SELECT * FROM ", tempTableNames43),collapse = "\n  UNION ALL\n"),
                  "\n) temp")
    sql <- SqlRender::renderSql(sql = sql,
                                cdm_schema = cdmDatabaseSchema)$sql
    sql <- SqlRender::translateSql(sql = sql,
                                   targetDialect = connectionDetails$dbms)$sql

    DatabaseConnector::executeSql(conn, sql, progressBar = TRUE, reportOverallTime = TRUE)

    dbDisconnect(conn)

    }
  }
