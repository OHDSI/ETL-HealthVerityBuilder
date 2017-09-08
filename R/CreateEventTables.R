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

#' Run the query to create the event tables from the STEM table. These include condition_occurrence, procedure_occurrence
#' drug_exposure, device_exposure, measurement and observation.
#'
#' This function will call and run the sql file to create the drug_exposure table from the STEM table.
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

createEventTables <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    oracleTempSchema = NULL){

  conn <- connect(connectionDetails)

  createConditionOccurrenceTable(connectionDetails = connectionDetails,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema)

  createProcedureOccurrenceTable(connectionDetails = connectionDetails,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema)

  createDrugExposureTable(connectionDetails = connectionDetails,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema)

  createDeviceExposureTable(connectionDetails = connectionDetails,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema)

  createMeasurementTable(connectionDetails = connectionDetails,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema)

  createObservationTable(connectionDetails = connectionDetails,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema)

}
