{{ config(
    materialized='incremental',
    tags=['experimental', 'SOH']
) }}

WITH base AS (
    SELECT
        v.device_id,
        v.ts,
        v.soc,
        v.current,
        v.nominal_capacity,
        v.battery_state,
        v.q_delta,
        v.q_throughput,
        v.temp01, v.temp02, v.temp03, v.temp04, v.temp05, v.temp06,
        v.temp07, v.temp08, v.temp09, v.temp10, v.temp11, v.temp12,
        v.temp13, v.temp14, v.temp15, v.temp16
    FROM {{ ref('inter_battery_variable_mapping') }} v
    WHERE v.current > 0
    {% if is_incremental() %}
        AND v.ts >= (SELECT COALESCE(MAX(ts), '1970-01-01') FROM {{ this }})
        AND NOT EXISTS (
            SELECT 1 FROM {{ this }} t
            WHERE t.device_id = v.device_id
              AND t.ts = v.ts
        )
    {% endif %}
),

unpivoted AS (
    SELECT
        b.device_id,
        b.ts,
        b.current,
        b.nominal_capacity,
        b.q_delta,
        b.q_throughput,
        t.value.cell_no AS cell_no,
        t.value.temp_c AS temp_c
    FROM base b
    CROSS JOIN LATERAL UNNEST([
        STRUCT_PACK(cell_no := 'cell01', temp_c := b.temp01),
        STRUCT_PACK(cell_no := 'cell02', temp_c := b.temp02),
        STRUCT_PACK(cell_no := 'cell03', temp_c := b.temp03),
        STRUCT_PACK(cell_no := 'cell04', temp_c := b.temp04),
        STRUCT_PACK(cell_no := 'cell05', temp_c := b.temp05),
        STRUCT_PACK(cell_no := 'cell06', temp_c := b.temp06),
        STRUCT_PACK(cell_no := 'cell07', temp_c := b.temp07),
        STRUCT_PACK(cell_no := 'cell08', temp_c := b.temp08),
        STRUCT_PACK(cell_no := 'cell09', temp_c := b.temp09),
        STRUCT_PACK(cell_no := 'cell10', temp_c := b.temp10),
        STRUCT_PACK(cell_no := 'cell11', temp_c := b.temp11),
        STRUCT_PACK(cell_no := 'cell12', temp_c := b.temp12),
        STRUCT_PACK(cell_no := 'cell13', temp_c := b.temp13),
        STRUCT_PACK(cell_no := 'cell14', temp_c := b.temp14),
        STRUCT_PACK(cell_no := 'cell15', temp_c := b.temp15),
        STRUCT_PACK(cell_no := 'cell16', temp_c := b.temp16)
    ]) AS t(value)
    WHERE
        b.device_id IS NOT NULL
        AND b.device_id != ''
        AND b.ts IS NOT NULL
        AND b.current IS NOT NULL
        AND b.q_delta IS NOT NULL
        AND t.value.temp_c IS NOT NULL
),

bucketed AS (
    SELECT
        *,
        ABS(current) / nominal_capacity AS c_rate,

        CASE
            WHEN current >= 0 AND (ABS(current) / nominal_capacity) <= 0.5 THEN '0 to 0.5C'
            WHEN current >= 0 AND (ABS(current) / nominal_capacity) <= 1.0 THEN '0.5 to 1C'
            WHEN current >= 0 THEN '>1C'
        END AS c_rate_bucket,

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

        -- Extract year from timestamp
        EXTRACT(YEAR FROM ts) AS year,

        -- Determine season based on month
        CASE
            WHEN EXTRACT(MONTH FROM ts) IN (3, 4, 5, 6) THEN 'summer'
            WHEN EXTRACT(MONTH FROM ts) IN (7, 8, 9, 10) THEN 'rainy'
            WHEN EXTRACT(MONTH FROM ts) IN (11, 12, 1, 2) THEN 'winter'
        END AS season,

        -- Create season_year column (for winter, Nov-Dec belong to next year's winter)
        CASE
            WHEN EXTRACT(MONTH FROM ts) IN (3, 4, 5, 6) THEN 'summer_' || EXTRACT(YEAR FROM ts)
            WHEN EXTRACT(MONTH FROM ts) IN (7, 8, 9, 10) THEN 'rainy_' || EXTRACT(YEAR FROM ts)
            WHEN EXTRACT(MONTH FROM ts) IN (1, 2) THEN 'winter_' || EXTRACT(YEAR FROM ts)
            WHEN EXTRACT(MONTH FROM ts) IN (11, 12) THEN 'winter_' || (EXTRACT(YEAR FROM ts) + 1)
        END AS season_year

    FROM unpivoted
    WHERE temp_c BETWEEN -10 AND 55
)

SELECT *
FROM bucketed
WHERE temp_bucket != 'out_of_range'
