{{ config(
    materialized='incremental',
    tags=['experimental', 'SOH']
) }}

WITH src AS (
    SELECT *
    FROM {{ ref('inter_battery_variable_mapping') }}
    {% if is_incremental() %}
        WHERE ts >= (SELECT COALESCE(MAX(ts), '1970-01-01') FROM {{ this }})
        AND NOT EXISTS (
            SELECT 1
            FROM {{ this }} t
            WHERE t.device_id = {{ ref('inter_battery_variable_mapping') }}.device_id
              AND t.ts = {{ ref('inter_battery_variable_mapping') }}.ts
        )
    {% endif %}
),

filtered AS (
    SELECT *
    FROM src
    WHERE battery_state IN (0, 3, 8, 20)
),

unpivoted AS (
    SELECT
        f.device_id,
        f.ts,
        f.soc,
        f.current,
        f.battery_state,
        t.value.cell_no AS cell_no,
        t.value.temp_c AS temp_c
    FROM filtered f
    CROSS JOIN LATERAL UNNEST([
        STRUCT_PACK(cell_no := 'cell01', temp_c := f.temp01),
        STRUCT_PACK(cell_no := 'cell02', temp_c := f.temp02),
        STRUCT_PACK(cell_no := 'cell03', temp_c := f.temp03),
        STRUCT_PACK(cell_no := 'cell04', temp_c := f.temp04),
        STRUCT_PACK(cell_no := 'cell05', temp_c := f.temp05),
        STRUCT_PACK(cell_no := 'cell06', temp_c := f.temp06),
        STRUCT_PACK(cell_no := 'cell07', temp_c := f.temp07),
        STRUCT_PACK(cell_no := 'cell08', temp_c := f.temp08),
        STRUCT_PACK(cell_no := 'cell09', temp_c := f.temp09),
        STRUCT_PACK(cell_no := 'cell10', temp_c := f.temp10),
        STRUCT_PACK(cell_no := 'cell11', temp_c := f.temp11),
        STRUCT_PACK(cell_no := 'cell12', temp_c := f.temp12),
        STRUCT_PACK(cell_no := 'cell13', temp_c := f.temp13),
        STRUCT_PACK(cell_no := 'cell14', temp_c := f.temp14),
        STRUCT_PACK(cell_no := 'cell15', temp_c := f.temp15),
        STRUCT_PACK(cell_no := 'cell16', temp_c := f.temp16)
    ]) AS t(value)
    WHERE 
        f.device_id IS NOT NULL
        AND f.device_id != ''
        AND f.ts IS NOT NULL
        AND f.soc IS NOT NULL
        AND f.current IS NOT NULL
        AND t.value.temp_c IS NOT NULL
),

bucketed AS (
    SELECT
        *,
        CASE
            WHEN soc < 20 THEN '0-20'
            WHEN soc < 40 THEN '20-40'
            WHEN soc < 60 THEN '40-60'
            WHEN soc < 80 THEN '60-80'
            WHEN soc <= 100 THEN '80-100'
            ELSE 'out_of_range'
        END AS soc_bucket,

        CASE
            WHEN temp_c BETWEEN -10 AND -5 THEN '-10 to -5'
            WHEN temp_c BETWEEN -5 AND 0 THEN '-5 to 0'
            WHEN temp_c BETWEEN 0 AND 5 THEN '0 to 5'
            WHEN temp_c BETWEEN 5 AND 10 THEN '5 to 10'
            WHEN temp_c BETWEEN 10 AND 15 THEN '10 to 15'
            WHEN temp_c BETWEEN 15 AND 25 THEN '15 to 25'
            WHEN temp_c BETWEEN 25 AND 35 THEN '25 to 35'
            WHEN temp_c BETWEEN 35 AND 40 THEN '35 to 40'
            WHEN temp_c BETWEEN 40 AND 45 THEN '40 to 45'
            WHEN temp_c BETWEEN 45 AND 50 THEN '45 to 50'
            WHEN temp_c BETWEEN 50 AND 55 THEN '50 to 55'
            ELSE 'out_of_range'
        END AS temp_bucket,
        
        CASE
            WHEN EXTRACT(MONTH FROM ts) IN (3, 4, 5, 6) THEN 'summer'
            WHEN EXTRACT(MONTH FROM ts) IN (7, 8, 9, 10) THEN 'rainy'
            WHEN EXTRACT(MONTH FROM ts) IN (11, 12, 1, 2) THEN 'winter'
        END AS season,

        CASE
            WHEN EXTRACT(MONTH FROM ts) IN (1, 2) THEN EXTRACT(YEAR FROM ts) - 1
            ELSE EXTRACT(YEAR FROM ts)
        END AS year,

        CASE
            WHEN EXTRACT(MONTH FROM ts) IN (3, 4, 5, 6) THEN 'summer_' || EXTRACT(YEAR FROM ts)
            WHEN EXTRACT(MONTH FROM ts) IN (7, 8, 9, 10) THEN 'rainy_' || EXTRACT(YEAR FROM ts)
            WHEN EXTRACT(MONTH FROM ts) IN (11, 12) THEN 'winter_' || EXTRACT(YEAR FROM ts)
            WHEN EXTRACT(MONTH FROM ts) IN (1, 2) THEN 'winter_' || (EXTRACT(YEAR FROM ts) - 1)
        END AS season_year

    FROM unpivoted
    WHERE temp_c BETWEEN -10 AND 55
)

SELECT *
FROM bucketed
WHERE temp_bucket != 'out_of_range'
