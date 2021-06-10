{{ config(materialized='table', unique_key="service_id") }}

SELECT CAST((REGEXP_MATCHES(meta_data, '^\d{1,}'))[1] AS integer) AS service_id,
(REGEXP_MATCHES(meta_data, '_(.*?)_'))[1] AS service_name_nl,
(REGEXP_MATCHES(meta_data, '_(.*?)_(.*?)_'))[2] AS service_name_en
FROM staging_events
WHERE event_type = 'proposed'
GROUP BY service_id, service_name_nl, service_name_en
