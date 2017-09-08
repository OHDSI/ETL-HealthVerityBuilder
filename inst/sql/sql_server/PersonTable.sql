/*PERSON*/
/*********************************************************************************************/
IF OBJECT_ID('@cdm_schema.PERSON') IS NOT NULL DROP TABLE @cdm_schema.[PERSON]
CREATE TABLE @cdm_schema.[PERSON]
WITH (
	CLUSTERED COLUMNSTORE INDEX,
	DISTRIBUTION = HASH(PERSON_ID)
)
AS
select person.person_id, gender.gender_concept_id, SUBSTRING(cast(yob.year_of_birth as varchar),1,4) AS YEAR_OF_BIRTH,
	null as month_of_birth, null as day_of_birth, null as time_of_birth,0 as race_concept_id,
	0 as ethnicity_concept_id, most_recent_location.location_id, null as provider_id, null as care_site_id,
	person.person_source_value, gender.gender_source_value, 0 as gender_source_concept_id,
	null as race_source_value, 0 as race_source_concept_id, null as ethnicity_source_value,
	0 as ethnicity_source_concept_id
from (
	select hvid as person_source_value, row_number() over (order by hvid asc) as person_id
	from (
		select distinct hvid
		from (
			select hvid
			from @source_schema.medical_claims
			WHERE HVID IS NOT NULL
			UNION ALL
			select hvid
			from @source_schema.pharmacy_claims
			WHERE HVID IS NOT NULL
		) t1
	) t2
) person

inner join (
	select hvid as person_source_value, patient_gender as gender_source_value,
		case
			when patient_gender = 'M' then 8507
			when patient_gender = 'F' then 8532
            else 0
		end as gender_concept_id
	from (
		select hvid, patient_gender, row_number() over (partition by hvid order by num_records desc) as rn1
		from (
			select hvid, patient_gender, sum(num_records) as num_records
			from (
				select hvid, patient_gender, count(hvid) as num_records
				from @source_schema.medical_claims
				group by hvid, patient_gender
				UNION ALL
				select hvid, patient_gender, count(hvid) as num_records
				from @source_schema.pharmacy_claims
				group by hvid, patient_gender
			) t1
			group by hvid, patient_gender
		) t2
	) t3
	where t3.rn1 = 1
) gender
	on person.person_source_value = gender.person_source_value
inner join (
	select hvid as person_source_value, cast(patient_year_of_birth as int) as year_of_birth
	from (
		select hvid, patient_year_of_birth, row_number() over (partition by hvid order by num_records desc) as rn1
		from (
			select hvid, patient_year_of_birth, sum(num_records) as num_records
			from (
				select hvid, cast(patient_year_of_birth as varchar) as patient_year_of_birth, count(hvid) as num_records
				from @source_schema.medical_claims
				where cast(patient_year_of_birth as varchar) <> 'NULL'
				group by hvid, cast(patient_year_of_birth as varchar)
				union ALL
				select hvid, cast(patient_year_of_birth as varchar) as patient_year_of_birth, count(hvid) as num_records
				from @source_schema.pharmacy_claims
				where cast(patient_year_of_birth as varchar) <> 'NULL'
				group by hvid,  cast(patient_year_of_birth as varchar)
			) t1
			group by hvid, patient_year_of_birth
		) t2
	) t3
	where t3.rn1 = 1
) yob
	on person.person_source_value = yob.person_source_value
left join (
	select t2.hvid as person_source_value, t2.location_source_value, location.location_id
	from (
		select hvid, location_source_value, row_number() over (partition by hvid order by date_service desc) as rn1
		from (
			select hvid, date_service,
				cast(patient_zip3 as varchar) + ' ' + cast(patient_state as varchar) as location_source_value
			from @source_schema.medical_claims
			union ALL
			select hvid, date_service,
				cast(patient_zip3 as varchar) + ' ' + cast(patient_state as varchar) as location_source_value
			from @source_schema.pharmacy_claims
			) t1
	) t2
		inner join @cdm_schema.location on location.location_source_value = t2.location_source_value
	where t2.rn1 = 1
) most_recent_location
	on person.person_source_value = most_recent_location.person_source_value
where yob.year_of_birth > 0 and gender_concept_id <> 0;
