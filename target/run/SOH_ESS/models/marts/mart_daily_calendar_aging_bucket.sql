

    insert into "dev"."main"."mart_daily_calendar_aging_bucket" ("device_id", "ts", "soc", "current", "battery_state", "v_min", "cell_no", "temp_c", "soc_bucket", "temp_bucket")
    (
        select "device_id", "ts", "soc", "current", "battery_state", "v_min", "cell_no", "temp_c", "soc_bucket", "temp_bucket"
        from "mart_daily_calendar_aging_bucket__dbt_tmp20260421203103842735"
    )
  