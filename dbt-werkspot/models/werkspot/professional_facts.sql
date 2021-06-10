{{ config(materialized='table', unique_key="id") }}

WITH unable_to_propose AS (
	SELECT professional_id_anonymized,
	COUNT(*) AS number_times_unable_to_propose
	FROM staging_events
	WHERE event_type = 'became_unable_to_propose'
	GROUP BY professional_id_anonymized
)
SELECT id,
count(DISTINCT event_id) AS number_propositions,
DATE_PART('day', pr.date_first_able_propose - pr.date_creation) AS days_between_creation_and_activation,
utp.number_times_unable_to_propose
FROM {{ref('professionals')}} pr
LEFT JOIN staging_events se ON se.professional_id_anonymized = pr.id
LEFT JOIN unable_to_propose utp ON utp.professional_id_anonymized = pr.id
WHERE se.event_type = 'proposed'
GROUP BY id, date_first_able_propose, date_creation, number_times_unable_to_propose
