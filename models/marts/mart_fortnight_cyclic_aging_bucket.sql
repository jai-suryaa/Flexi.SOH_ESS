{{ config(
    materialized='incremental',
    unique_key=['device_id', 'cell_no', 'temp_bucket', 'c_rate_bucket', 'season_year'],
    tags=['experimental', 'SOH']
) }}

WITH buckets AS (
    SELECT *
    FROM {{ ref('mart_daily_cyclic_aging_bucket') }}
    {% if is_incremental() %}
        WHERE season_year IN (
            SELECT DISTINCT season_year
            FROM {{ ref('mart_daily_cyclic_aging_bucket') }}
            WHERE ts >= '{{ var("start_date") }}'::date
              AND ts <  '{{ var("end_date") }}'::date
        )
    {% endif %}
),

final_aggregation AS (
    SELECT
        device_id,
        temp_bucket,
        c_rate_bucket,
        cell_no,
        year,
        season,
        season_year,
        AVG(c_rate) AS avg_c_rate,
        AVG(temp_c) AS avg_temp,
        SUM(q_delta) AS total_q_throughput
    FROM buckets
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    device_id,
    temp_bucket,
    c_rate_bucket,
    cell_no,
    year,
    season,
    season_year,
    avg_c_rate,
    avg_temp,
    total_q_throughput,
    CAST(NULL AS DOUBLE) AS q_loss,
    '{{ var("process_date") }}'::date AS ts
FROM final_aggregation
ORDER BY 1, 2, 3, 4, 7
