import subprocess
from datetime import datetime, timedelta
import sys
import duckdb
import math
import pandas as pd
import os
import traceback

NUM_DAYS = 2
DBT_PROJECT_DIR = "C:/EOL/SOH/Flexi.SOH_ESS"
DB_FILE = "dev.duckdb"

A = 0.03
Ea = 16500
n = 0.3
m = 0.7

A_cal = 2.0 * (10 ** (-8))
B = 3485
C = 1.1
D = 2


def run_dbt_command(command_args):
    try:
        result = subprocess.run(
            ["dbt"] + command_args,
            cwd=DBT_PROJECT_DIR,
            text=True,
            capture_output=True,
            shell=True
        )
        print(result.stdout)
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("dbt failed")

    except Exception as e:
        print(f"Error running dbt command: {' '.join(command_args)}")
        print(e)
        exit(1)


def get_start_date():
    if len(sys.argv) > 1:
        try:
            return datetime.strptime(sys.argv[1], "%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            exit(1)

    date_input = input("Enter start date (YYYY-MM-DD): ").strip()
    try:
        return datetime.strptime(date_input, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        exit(1)


def compute_q_loss_cyclic(row):
    T = row["avg_temp"] + 273.15
    return float(
        A * math.exp(-Ea / (8.314 * T)) *
        (row["avg_c_rate"] ** n) *
        (row["total_q_throughput"] ** m)
    )


def compute_q_loss_calendar(row):
    T = row["avg_temp"] + 273.15
    return float(
        A_cal *
        math.exp(B / T) *
        C *
        math.exp(D * (row["avg_soc"] / 100)) *
        (row["total_rest_hours"] ** 0.5)
    )


def run_q_loss_calculations():
    print("\nRunning post-dbt q_loss calculations...")
    db_path = os.path.join(DBT_PROJECT_DIR, DB_FILE)
    con = duckdb.connect(db_path)

    try:
        print("\nProcessing cyclic aging...")

        cyclic_df = con.execute(""" SELECT device_id, cell_no, temp_bucket, c_rate_bucket, avg_temp, avg_c_rate, total_q_throughput FROM mart_fortnight_cyclic_aging_bucket """).df()

        if cyclic_df.empty:
            print("Cyclic aging table is empty. Skipping calculations.")
        else:
            print(f"Applying cyclic q_loss function to {len(cyclic_df)} rows...")
            cyclic_df["q_loss"] = cyclic_df.apply(compute_q_loss_cyclic, axis=1)

            con.register("cyclic_updates_view", cyclic_df)
            con.execute("""
                UPDATE mart_fortnight_cyclic_aging_bucket AS t
                SET q_loss = u.q_loss
                FROM cyclic_updates_view AS u
                WHERE t.device_id    = u.device_id
                  AND t.cell_no      = u.cell_no
                  AND t.temp_bucket  = u.temp_bucket
                  AND t.c_rate_bucket = u.c_rate_bucket
            """)
            con.unregister("cyclic_updates_view")
            print(f"Cyclic aging table updated with {len(cyclic_df)} rows.")

        print("\nProcessing calendar aging...")

        cal_df = con.execute(""" SELECT device_id, cell_no, soc_bucket, temp_bucket, avg_temp, avg_soc, total_rest_hours FROM mart_fortnight_calender_aging_bucket """).df()

        if cal_df.empty:
            print("Calendar aging table is empty. Skipping calculations.")
        else:
            print(f"Applying calendar q_loss function to {len(cal_df)} rows...")
            cal_df["q_loss"] = cal_df.apply(compute_q_loss_calendar, axis=1)

            con.register("calendar_updates_view", cal_df)
            con.execute("""
                UPDATE mart_fortnight_calender_aging_bucket AS t
                SET q_loss = u.q_loss
                FROM calendar_updates_view AS u
                WHERE t.device_id   = u.device_id
                  AND t.cell_no     = u.cell_no
                  AND t.soc_bucket  = u.soc_bucket
                  AND t.temp_bucket = u.temp_bucket
            """)
            con.unregister("calendar_updates_view")
            print(f"Calendar aging table updated with {len(cal_df)} rows.")

    except BaseException as e:
        print(f"\nError type: {type(e)}")
        print(f"Error details: {e}")
        traceback.print_exc()

    finally:
        con.close()
        print("\nDatabase connection closed.")


def main():
    start_date = get_start_date()
    end_date = start_date + timedelta(days=NUM_DAYS)

    print(f"Running staging for {NUM_DAYS} consecutive days starting {start_date.strftime('%Y-%m-%d')}...")

    for i in range(NUM_DAYS):
        current_date = start_date + timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"\n--- Day {i + 1}: {date_str} ---")

        run_dbt_command(["run", "--models", "stg_battery_raw_parquet_data", "inter_battery_variable_mapping", "mart_daily_cyclic_aging_bucket", "mart_daily_calendar_aging_bucket", "--vars", f'{{"process_date": "{date_str}"}}'])

    print(f"\nRunning {NUM_DAYS}-day aggregation and q_loss calculation...")

    run_dbt_command(["run", "--models", "mart_fortnight_cyclic_aging_bucket", "mart_fortnight_calender_aging_bucket", "--vars", f'{{"start_date": "{start_date.strftime("%Y-%m-%d")}", "end_date": "{end_date.strftime("%Y-%m-%d")}", "process_date": "{start_date.strftime("%Y-%m-%d")}"}}'])

    run_q_loss_calculations()

    print("\nWorkflow completed successfully!")


if __name__ == "__main__":
    main()