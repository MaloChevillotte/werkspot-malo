{{ config(materialized='table') }}

WITH generated_dates AS (
   {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-01-02' as date)",
    end_date="cast('2020-03-10' as date)"
   )
}}
),
ordered_events AS (
  SELECT professional_id_anonymized, event_type, created_at,
  lead(event_type) OVER (PARTITION BY professional_id_anonymized ORDER BY created_at) AS next_step
  FROM staging_events
  WHERE event_type IN ('became_able_to_propose', 'became_unable_to_propose')
  ORDER BY professional_id_anonymized, created_at
), ordered_filtered_events AS (
  SELECT *
  FROM ordered_events
  WHERE event_type <> next_step OR next_step IS NULL
), variation AS (
  SELECT date_trunc('day', created_at) AS day,
  SUM(CASE WHEN event_type = 'became_able_to_propose' THEN 1 ELSE 0 END) + SUM(CASE WHEN event_type = 'became_unable_to_propose' THEN -1 ELSE 0 END) AS variation
  FROM ordered_filtered_events
  GROUP BY day
)
SELECT date_day,
sum(variation) OVER (ORDER BY date_day) AS active_professional_count
FROM generated_dates
LEFT JOIN variation ON variation.day = generated_dates.date_day
ORDER BY generated_dates.date_day, variation.day
