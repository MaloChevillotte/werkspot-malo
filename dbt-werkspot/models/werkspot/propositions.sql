{{ config(materialized='table', unique_key="event_id") }}

SELECT event_id,
professional_id_anonymized,
created_at,
CAST((REGEXP_MATCHES(meta_data, '\d{1,}.\d+$'))[1] AS numeric) AS fee,
CAST((REGEXP_MATCHES(meta_data, '^\d{1,}'))[1] AS integer) AS service_id
FROM staging_events
WHERE event_type = 'proposed'
