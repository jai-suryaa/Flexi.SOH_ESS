

    insert into "dev"."main"."inter_battery_variable_mapping" ("device_id", "ts", "soc", "current", "nominal_capacity", "battery_state", "v_min", "v_max", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15", "v16", "temp01", "temp02", "temp03", "temp04", "temp05", "temp06", "temp07", "temp08", "temp09", "temp10", "temp11", "temp12", "temp13", "temp14", "temp15", "temp16", "avg_temp", "q_delta", "q_throughput")
    (
        select "device_id", "ts", "soc", "current", "nominal_capacity", "battery_state", "v_min", "v_max", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15", "v16", "temp01", "temp02", "temp03", "temp04", "temp05", "temp06", "temp07", "temp08", "temp09", "temp10", "temp11", "temp12", "temp13", "temp14", "temp15", "temp16", "avg_temp", "q_delta", "q_throughput"
        from "inter_battery_variable_mapping__dbt_tmp20260421203036929406"
    )
  