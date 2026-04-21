

    insert into "dev"."main"."mart_daily_cyclic_aging_bucket" ("device_id", "ts", "current", "nominal_capacity", "q_delta", "q_throughput", "cell_no", "temp_c", "c_rate", "c_rate_bucket", "temp_bucket", "year", "season", "season_year")
    (
        select "device_id", "ts", "current", "nominal_capacity", "q_delta", "q_throughput", "cell_no", "temp_c", "c_rate", "c_rate_bucket", "temp_bucket", "year", "season", "season_year"
        from "mart_daily_cyclic_aging_bucket__dbt_tmp20260421203104518593"
    )
  