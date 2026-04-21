
  
    
    

    create  table
      "dev"."main"."mart_fortnight_calender_aging_bucket"
  
    as (
      

WITH idle_periods_unpivoted AS (
    SELECT *
    FROM "dev"."main"."mart_daily_calendar_aging_bucket"
    
),

all_events_with_duration AS (
    SELECT 
        device_id,
        ts,
        EXTRACT(EPOCH FROM (
            LEAD(ts, 1) OVER (PARTITION BY device_id ORDER BY ts) - ts
        )) AS duration_seconds
    FROM "dev"."main"."inter_battery_variable_mapping"
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
        cell_no,
        soc_bucket,
        temp_bucket,
        AVG(soc)                        AS avg_soc,
        AVG(temp_c)                     AS avg_temp,
        SUM(duration_seconds) / 3600.0  AS total_rest_hours
    FROM idle_with_duration
    GROUP BY
        device_id,
        cell_no,
        soc_bucket,
        temp_bucket
)

SELECT
    device_id,
    cell_no,
    soc_bucket,
    temp_bucket,
    avg_soc,
    avg_temp,
    total_rest_hours,
    CAST(NULL AS DOUBLE)                AS q_loss,
    '2024-10-10'::date   AS ts
FROM aggregated
ORDER BY device_id, cell_no, soc_bucket
    );
  
  
  