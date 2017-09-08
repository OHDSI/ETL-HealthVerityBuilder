/*********************************************************************************************/
  /*STEP 2: Create lookup tables*/
  /*LOCATION*/
  /*********************************************************************************************/

IF OBJECT_ID('@cdm_schema.LOCATION') IS NOT NULL DROP TABLE @cdm_schema.[LOCATION]
CREATE TABLE @cdm_schema.[LOCATION]
WITH (
  CLUSTERED COLUMNSTORE INDEX,
  DISTRIBUTION = HASH(LOCATION_ID)
)
  AS select row_number() over (order by location_source_value asc) as location_id,
  NULL AS ADDRESS_1,
  NULL AS ADDRESS_2,
  NULL AS CITY,
  State,
  zip,
  NULL AS COUNTY,
  location_source_value
  from (
    select distinct zip, state, location_source_value
    from (
      select cast(patient_zip3 as varchar) as zip,
      cast(patient_state as varchar) as state,
      CASE
      WHEN PATIENT_ZIP3 IS NULL AND PATIENT_STATE IS NOT NULL
      THEN cast(patient_state as varchar)
      WHEN PATIENT_ZIP3 IS NOT NULL AND PATIENT_STATE IS NULL
      THEN cast(patient_zip3 as varchar)
      WHEN PATIENT_ZIP3 IS NOT NULL AND PATIENT_STATE IS NOT NULL
      THEN cast(patient_zip3 as varchar) + ' ' + cast(patient_state as varchar)
      ELSE NULL END as location_source_value
      from @source_schema.medical_claims
      UNION ALL
      select cast(patient_zip3 as varchar) as zip,
      cast(patient_state as varchar) as state,
      CASE
      WHEN PATIENT_ZIP3 IS NULL AND PATIENT_STATE IS NOT NULL
      THEN cast(patient_state as varchar)
      WHEN PATIENT_ZIP3 IS NOT NULL AND PATIENT_STATE IS NULL
      THEN cast(patient_zip3 as varchar)
      WHEN PATIENT_ZIP3 IS NOT NULL AND PATIENT_STATE IS NOT NULL
      THEN cast(patient_zip3 as varchar) + ' ' + cast(patient_state as varchar)
      ELSE NULL END as location_source_value
      from @source_schema.pharmacy_claims
  ) t1
) t2;
