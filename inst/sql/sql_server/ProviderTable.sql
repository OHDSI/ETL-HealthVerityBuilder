/*PROVIDER*/
/*********************************************************************************************/

IF OBJECT_ID('@cdm_schema.PROVIDER') IS NOT NULL DROP TABLE @cdm_schema.[PROVIDER]
CREATE TABLE @cdm_schema.[PROVIDER]
WITH (
	CLUSTERED COLUMNSTORE INDEX,
	DISTRIBUTION = HASH(PROVIDER_ID)
)
AS
SELECT PROVIDER_ID,
	NULL AS PROVIDER_NAME,
	PROVIDER_SOURCE_VALUE AS NPI,
	NULL AS DEA,
	0 AS SPECIALTY_CONCEPT_ID,
	NULL AS CARE_SITE_ID,
	NULL AS YEAR_OF_BIRTH,
	0 AS GENDER_CONCEPT_ID,
	PROVIDER_SOURCE_VALUE,
	NULL AS SPECIALTY_SOURCE_VALUE,
	0 AS SPECIALTY_SOURCE_CONCEPT_ID,
	NULL AS GENDER_SOURCE_VALUE,
	0 AS GENDER_SOURCE_CONCEPT_ID
FROM (
	SELECT PROVIDER_SOURCE_VALUE, ROW_NUMBER() OVER (ORDER BY PROVIDER_SOURCE_VALUE ASC) AS PROVIDER_ID
	FROM (
		SELECT DISTINCT PROVIDER_SOURCE_VALUE
		FROM (
			SELECT PROV_RENDERING_NPI AS PROVIDER_SOURCE_VALUE
			FROM @source_schema.MEDICAL_CLAIMS
			UNION ALL
			SELECT PROV_DISPENSING_NPI AS PROVIDER_SOURCE_VALUE
			FROM @source_schema.PHARMACY_CLAIMS
		) T1
	) T2
) PROVIDER;
