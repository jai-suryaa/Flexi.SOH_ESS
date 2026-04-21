
  
    
    

    create  table
      "dev"."main"."mart_fortnight_cyclic_aging_bucket"
  
    as (
      

WITH buckets AS (
    SELECT *
    FROM "dev"."main"."mart_daily_cyclic_aging_bucket"
    
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
        AVG(c_rate)     AS avg_c_rate,
        AVG(temp_c)     AS avg_temp,
        SUM(q_delta)    AS total_q_throughput
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
    CAST(NULL AS DOUBLE)                AS q_loss,
    '2024-10-10'::date   AS ts
FROM final_aggregation
ORDER BY 1, 2, 3, 4, 7
    );
  
  
  