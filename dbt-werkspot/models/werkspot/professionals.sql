{{ config(materialized='table', unique_key="id") }}

with account_creation AS (
	SELECT professional_id_anonymized AS id,
	MIN(created_at) AS date_creation
	FROM staging_events
	WHERE event_type = 'created_account'
	GROUP BY professional_id_anonymized
), first_time_able_to_propose AS (
	SELECT professional_id_anonymized AS id,
	MIN(created_at) AS date_first_able_propose
	FROM staging_events
	WHERE event_type = 'became_able_to_propose'
	GROUP BY professional_id_anonymized
), last_time_able_to_propose AS (
	SELECT professional_id_anonymized AS id,
	MAX(created_at) AS date_last_able_propose
	FROM staging_events
	WHERE event_type = 'became_able_to_propose'
	GROUP BY professional_id_anonymized
), last_time_unable_to_propose AS (
	SELECT professional_id_anonymized AS id,
	MAX(created_at) AS date_last_unable_propose
	FROM staging_events
	WHERE event_type = 'became_unable_to_propose'
	GROUP BY professional_id_anonymized
)
SELECT ac.id,
ac.date_creation,
ftatp.date_first_able_propose,
ltatp.date_last_able_propose,
ltutp.date_last_unable_propose,
CASE
	WHEN ftatp.date_first_able_propose IS NULL THEN 'No'
	WHEN ftatp.date_first_able_propose IS NOT NULL AND ltutp.date_last_unable_propose IS NULL THEN 'Yes'
	WHEN ltatp.date_last_able_propose > ltutp.date_last_unable_propose THEN 'Yes'
	ELSE 'No'
	END AS is_currently_active
FROM account_creation ac
LEFT JOIN first_time_able_to_propose ftatp ON ftatp.id = ac.id
LEFT JOIN last_time_able_to_propose ltatp ON ltatp.id = ac.id
LEFT JOIN last_time_unable_to_propose ltutp ON ltutp.id = ac.id

GROUP BY ac.id, date_creation, date_first_able_propose, date_last_able_propose, date_last_unable_propose
