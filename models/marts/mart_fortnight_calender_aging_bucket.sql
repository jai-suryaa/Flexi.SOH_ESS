{{ config(
    materialized='incremental',
    unique_key=['device_id', 'cell_no', 'soc_bucket', 'temp_bucket', 'season_year'],
    tags=['experimental', 'SOH']
) }}

WITH idle_periods_unpivoted AS (
    SELECT *
    FROM {{ ref('mart_daily_calendar_aging_bucket') }}
    {% if is_incremental() %}
        WHERE season_year IN (
            SELECT DISTINCT season_year
            FROM {{ ref('mart_daily_calendar_aging_bucket') }}
            WHERE ts >= '{{ var("start_date") }}'::date
              AND ts <  '{{ var("end_date") }}'::date
        )
    {% endif %}
),

all_events_with_duration AS (
    SELECT 
        device_id,
        ts,
        EXTRACT(EPOCH FROM (
            LEAD(ts, 1) OVER (PARTITION BY device_id ORDER BY ts) - ts
        )) AS duration_seconds
    FROM {{ ref('inter_battery_variable_mapping') }}
    {% if is_incremental() %}
        WHERE season_year IN (
            SELECT DISTINCT season_year
            FROM {{ ref('mart_daily_calendar_aging_bucket') }}
            WHERE ts >= '{{ var("start_date") }}'::date
              AND ts <  '{{ var("end_date") }}'::date
        )
    {% endif %}
),

idle_with_duration AS (
    SELECT
        idle.*,
        all_e.duration_seconds
    FROM idle_periods_unpivoted AS idle
    INNER JOIN all_events_with_duration AS all_e
        ON idle.device_id = all_e.device_id
        AND idle.ts = all_e.ts
    WHERE all_e.duration_seconds IS NOT NULL
),

aggregated AS (
    SELECT
        device_id,
        temp_bucket,
        soc_bucket,
        cell_no,
        year,
        season,
        season_year,
        AVG(soc)  AS avg_soc,
        AVG(temp_c)  AS avg_temp,
        SUM(duration_seconds) / 3600.0  AS total_rest_hours
    FROM idle_with_duration
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    device_id,
    temp_bucket,
    soc_bucket,
    cell_no,
    year,
    season,
    season_year,
    avg_soc,
    avg_temp,
    total_rest_hours,
    CAST(NULL AS DOUBLE) AS q_loss,
    '{{ var("process_date") }}'::date   AS ts
FROM aggregated
ORDER BY 1, 2, 3, 4, 7