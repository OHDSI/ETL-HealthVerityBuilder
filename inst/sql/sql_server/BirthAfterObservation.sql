/* Delete people born after start of observation*/
Delete
from @cdm_schema.person
where person_id in (
	select p.person_id
	from @cdm_schema.person p
	join @cdm_schema.observation_period op
		on p.person_id = op.person_id
	where year(observation_period_start_date) < year_of_birth
)

/* Delete excess rows from observation_period*/
Delete
from @cdm_schema.observation_period
where person_id not in (
	select p.person_id
	from @cdm_schema.person p
)

