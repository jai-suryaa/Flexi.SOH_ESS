
  
  create view "dev"."main"."stg_battery_raw_parquet_data__dbt_tmp" as (
    











SELECT *
FROM read_parquet(
    's3://tec-raw-data-archive/Year=2024/Month=10/Day=11/**/*.parquet',
    union_by_name=True
)
WHERE uniqueId != '__HIVE_DEFAULT_PARTITION__'
  );
