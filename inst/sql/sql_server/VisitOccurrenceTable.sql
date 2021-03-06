 /*Create VISIT_OCCURRENCE table */
/*********************************************************************/

IF OBJECT_ID('tempdb..@result_temp_vo') IS NOT NULL DROP TABLE @result_temp_vo
CREATE TABLE @result_temp_vo
WITH (
	CLUSTERED COLUMNSTORE INDEX,
	DISTRIBUTION = HASH(PERSON_ID)
) AS


SELECT VISIT_OCCURRENCE_ID,
	P.PERSON_ID,
	CASE
		WHEN AV.CLAIM_TYPE_NEW = 'IP'	THEN 9201
		WHEN AV.CLAIM_TYPE_NEW = 'OP'	THEN 9202
		WHEN AV.CLAIM_TYPE_NEW = 'ER'	THEN 9203
		WHEN AV.CLAIM_TYPE_NEW = 'LTC'	THEN 42898160
	END AS VISIT_CONCEPT_ID,
	AV.VISIT_START_DATE,
	AV.VISIT_END_DATE,
	NULL AS VISIT_TYPE_CONCEPT_ID,
	PR.PROVIDER_ID,
	NULL AS CARE_SITE_ID,
	AV.CLAIM_TYPE_NEW AS VISIT_SOURCE_VALUE,
	0 AS VISIT_SOURCE_CONCEPT_ID
FROM @cdm_schema.ALL_VISITS AS AV
	JOIN @cdm_schema.PERSON P
		ON AV.HVID = P.PERSON_SOURCE_VALUE
	LEFT JOIN @cdm_schema.PROVIDER PR
		ON AV.PROV_RENDERING_NPI = PR.PROVIDER_SOURCE_VALUE
WHERE VISIT_OCCURRENCE_ID IN (
	SELECT DISTINCT VISIT_OCCURRENCE_ID_NEW
	FROM @cdm_schema.FINAL_VISIT_IDS
) AND SUBSTRING(AV.HVID,1,@n_substring) = '@HVID_CHUNK'
