# ETL and builder for HealthVerity data
**Note: This builder was written for a data cut containing data from an open claims datasource and a closed claims datasource and is still under development.** 

## Dependencies
This package uses the [DatabaseConnector](https://github.com/OHDSI/DatabaseConnector) package and the [SqlRender](https://github.com/OHDSI/SqlRender) package. It also requires an instance of the [OMOP vocabulary](http://athena.ohdsi.org/).

## How to run 
The file `extras/TestCode.R` shows an example of how to run this builder. 

1. Using the `DatabaseConnector` package set the connection details to the server
2. Set the variables `cdmDatabaseSchema`, `sourceDatabaseSchema` and `vocabDatabaseSchema`
  a. In this example the cdm schema, source schema and vocab schema all exist on the same server
3. Run the `chunkData()` function. This will create a dataframe with a list of all the first digits of the HVIDs (set by nSubstring argument) in order to chunk the data to allow for more efficient processing
4. Run the `getIteraterNum()` function. This will find how many chunks the code has to run
5. Run the `createVocabTables()` function to create the vocabulary mapping tables.
6. Run the `createAllLookupTables()` function to create the LOCATION, PERSON, CARE_SITE and PROVIDER tables
7. Run the `createObservationPeriodTable()` function to create the OBSERVATION_PERIOD table. If createWebmdObsPeriod is set to TRUE then a separate OBSERVATION_PERIOD table will be created just for the time period the patients were in WebMD
8. 