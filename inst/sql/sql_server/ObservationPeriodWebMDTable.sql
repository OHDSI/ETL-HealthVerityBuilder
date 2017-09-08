/*OBSERVATION_PERIOD_WEBMD*/
/*********************************************************************************************/
IF XACT_STATE() = 1 COMMIT; CREATE TABLE  @result_temp_table
  WITH (LOCATION = USER_DB, DISTRIBUTION = HASH(PERSON_ID)) AS

WITH CTE_ALL_DATES AS (

	SELECT HVID, DATE_SERVICE AS DATE
	FROM @source_schema.MEDICAL_CLAIMS
	WHERE SUBSTRING(HVID,1,@n_substring) = '@HVID_CHUNK'
	and data_vendor = 'WebMD'

	UNION ALL

	SELECT HVID, DATE_SERVICE_END AS DATE
	FROM @source_schema.MEDICAL_CLAIMS
  WHERE SUBSTRING(HVID,1,@n_substring) = '@HVID_CHUNK'
  and data_vendor = 'WebMD'

	UNION ALL

	SELECT HVID, DATE_SERVICE AS DATE
	FROM @source_schema.PHARMACY_CLAIMS
  WHERE SUBSTRING(HVID,1,@n_substring) = '@HVID_CHUNK'
  and data_vendor = 'WebMD'

)

  SELECT
  		P.PERSON_ID,
  		OP.OBSERVATION_PERIOD_START_DATE,
  		OP.OBSERVATION_PERIOD_END_DATE,
  		44814724 AS PERIOD_TYPE_CONCEPT_ID
  FROM (
  		SELECT T1.HVID,
  				MIN(DATE) AS OBSERVATION_PERIOD_START_DATE,
  				MAX(DATE) AS OBSERVATION_PERIOD_END_DATE
  		FROM CTE_ALL_DATES T1
  		GROUP BY T1.HVID
  	) OP
  JOIN @cdm_schema.PERSON P
  ON P.PERSON_SOURCE_VALUE = OP.HVID
