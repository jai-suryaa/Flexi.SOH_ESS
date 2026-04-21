import subprocess
from datetime import datetime, timedelta
import sys
import duckdb
import math
import numpy as np
import pandas as pd
import os
import traceback
from scipy.optimize import curve_fit

NUM_DAYS = 15
DBT_PROJECT_DIR = "C:/EOL/SOH/Flexi.SOH_ESS"
DB_FILE = "dev.duckdb"

SOC_START_THRESHOLD = 30.0
SOC_END_THRESHOLD   = 99.0
MIN_CHARGE_CURRENT  = 5.0

IMBALANCE_TABLE_MV = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]
IMBALANCE_TABLE_AH = [0, 0.18, 0.32, 0.46, 0.56, 0.66, 0.80, 1.02, 1.34, 1.84, 2.60, 3.68]
VREF_IMBALANCE     = 3.58
SOH_INITIAL        = 1.0


def parse_args():
    args = sys.argv[1:]
    do_refit = "--refit" in args
    args = [a for a in args if a != "--refit"]

    if args:
        try:
            return datetime.strptime(args[0], "%Y-%m-%d"), do_refit
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            exit(1)

    date_input = input("Enter start date (YYYY-MM-DD): ").strip()
    try:
        return datetime.strptime(date_input, "%Y-%m-%d"), do_refit
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        exit(1)


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
        print(f"Error running dbt: {' '.join(command_args)}\n{e}")
        exit(1)


def fetch_constants(con, version=None):
    if version:
        cal_v = version
        cyc_v = version
    else:
        cal_latest = con.execute("""
            SELECT model_version FROM aging_constants_calendar
            ORDER BY fitted_on DESC LIMIT 1
        """).fetchone()
        cyc_latest = con.execute("""
            SELECT model_version FROM aging_constants_cyclic
            ORDER BY fitted_on DESC LIMIT 1
        """).fetchone()

        if cal_latest is None:
            raise ValueError("No calendar constants found in aging_constants_calendar.")
        if cyc_latest is None:
            raise ValueError("No cyclic constants found in aging_constants_cyclic.")

        cal_v = cal_latest[0]
        cyc_v = cyc_latest[0]

        if cal_v != cyc_v:
            print(f"  Warning: calendar version ({cal_v}) and cyclic version ({cyc_v}) differ. Using each independently.")

    cal_row = con.execute("""
        SELECT A_cal, B, C, D FROM aging_constants_calendar WHERE model_version = ?
    """, [cal_v]).fetchone()
    if cal_row is None:
        raise ValueError(f"No calendar constants for model_version='{cal_v}'")

    cyc_row = con.execute("""
        SELECT A, Ea, n, m FROM aging_constants_cyclic WHERE model_version = ?
    """, [cyc_v]).fetchone()
    if cyc_row is None:
        raise ValueError(f"No cyclic constants for model_version='{cyc_v}'")

    print(f"  Using constants: calendar={cal_v}, cyclic={cyc_v}")

    cal = {"A_cal": cal_row[0], "B": cal_row[1], "C": cal_row[2], "D": cal_row[3]}
    cyc = {"A": cyc_row[0], "Ea": cyc_row[1], "n": cyc_row[2], "m": cyc_row[3]}
    return cal, cyc, cal_v, cyc_v


def load_ocv_table(con):
    df = con.execute("""
        SELECT ocv_voltage, soc_percent FROM ocv_table ORDER BY ocv_voltage ASC
    """).df()
    if df.empty:
        raise ValueError("OCV table is empty.")
    return df["ocv_voltage"].to_numpy(), df["soc_percent"].to_numpy()


def voltage_to_soc(voltage, ocv_voltages, ocv_socs):
    return float(np.interp(voltage, ocv_voltages, ocv_socs))


def compute_q_loss_calendar(row, A_cal, B, C, D):
    T = row["avg_temp"] + 273.15
    return float(A_cal * math.exp(B / T) * C * math.exp(D * (row["avg_soc"] / 100)) * (row["total_rest_hours"] ** 0.5))


def compute_q_loss_cyclic(row, A, Ea, n, m):
    T = row["avg_temp"] + 273.15
    return float(A * math.exp(-Ea / (8.314 * T)) * (row["avg_c_rate"] ** n) * (row["total_q_throughput"] ** m))


def _get_next_capacity_id(con):
    return con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM pack_capacity_measurements").fetchone()[0]


def _get_next_qloss_id(con):
    return con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM q_loss_history").fetchone()[0]


def get_imbalance_ah(delta_v_mv):
    return float(np.interp(delta_v_mv, IMBALANCE_TABLE_MV, IMBALANCE_TABLE_AH))


def detect_and_estimate_capacity(con, start_date, end_date, ocv_voltages, ocv_socs, model_version):
    print("\nScanning for charge events...")

    raw_df = con.execute("""
        SELECT device_id, ts, v_min, current, soc, battery_state, temp01,
               nominal_capacity,
               v1, v2, v3, v4, v5, v6, v7, v8,
               v9, v10, v11, v12, v13, v14, v15, v16,
               v17, v18, v19, v20, v21, v22, v23, v24
        FROM inter_battery_variable_mapping
        WHERE ts >= ?::timestamp
          AND ts <  ?::timestamp
        ORDER BY device_id, ts
    """, [start_date, end_date]).df()

    if raw_df.empty:
        print("No data in window.")
        return 0

    cell_cols = [c for c in raw_df.columns if c.startswith("v") and c[1:].isdigit()]

    events_saved = 0
    next_id = _get_next_capacity_id(con)

    for device_id, group in raw_df.groupby("device_id"):
        group = group.sort_values("ts").reset_index(drop=True)

        in_charge = False
        charge_start_idx = None

        for i in range(len(group)):
            row = group.iloc[i]
            is_charging = row["current"] >= MIN_CHARGE_CURRENT

            if not in_charge and is_charging and row["soc"] < SOC_START_THRESHOLD:
                in_charge = True
                charge_start_idx = i

            elif in_charge:
                stopped = row["current"] < MIN_CHARGE_CURRENT
                reached = row["soc"] >= SOC_END_THRESHOLD

                if stopped or reached:
                    segment = group.iloc[charge_start_idx : i + 1]
                    actual_start_soc = float(segment.iloc[0]["soc"])
                    actual_end_soc = float(segment.iloc[-1]["soc"])
                    soc_rise = actual_end_soc - actual_start_soc

                    if len(segment) < 2 or soc_rise < 10.0:
                        in_charge = False
                        continue

                    ts_vals = pd.to_datetime(segment["ts"])
                    dt_hours = ts_vals.diff().dt.total_seconds().fillna(0) / 3600.0
                    coulombs_counted = float((segment["current"] * dt_hours).sum())

                    if coulombs_counted <= 0:
                        in_charge = False
                        continue

                    nominal_cap = float(segment["nominal_capacity"].iloc[0])
                    start_soc_frac = actual_start_soc / 100.0
                    leftover_ah = start_soc_frac * SOH_INITIAL * nominal_cap

                    delta_v_mv = 0.0
                    # if cell_cols:
                    #     for _, seg_row in segment.iterrows():
                    #         cell_voltages = seg_row[cell_cols].dropna().astype(float)
                    #         if cell_voltages.empty:
                    #             continue
                    #         if cell_voltages.max() == VREF_IMBALANCE:
                    #             delta_v_mv = (cell_voltages.max() - cell_voltages.min()) * 1000.0
                    #             break

                    if cell_cols:
                        for _, seg_row in segment.iterrows():
                            cell_voltages = seg_row[cell_cols].dropna().astype(float)
                            if cell_voltages.empty:
                                continue
                            vmax = cell_voltages.max()
                            vmin = cell_voltages.min()

                            if vmax == VREF_IMBALANCE:
                                delta_v_mv = (vmax - vmin) * 1000.0
                                closest_delta_v = delta_v_mv
                                break

                            diff = abs(vmax - VREF_IMBALANCE)
                            if diff < closest_diff:
                                closest_diff = diff
                                closest_delta_v = (vmax - vmin) * 1000.0

                        delta_v_mv = closest_delta_v if closest_delta_v is not None else 0.0

                    if delta_v_mv == 0:
                        in_charge = False
                        continue
                    imbalance_ah = get_imbalance_ah(max(0.0, delta_v_mv))
                    total_q_ah = leftover_ah + coulombs_counted + imbalance_ah

                    avg_temp = float(segment["temp01"].mean())
                    start_voltage = float(segment.iloc[0]["v_min"])
                    start_soc_ocv = voltage_to_soc(start_voltage, ocv_voltages, ocv_socs)

                    print(f"device={device_id} date={segment.iloc[0]['ts']} "
                          f"soc_rise={soc_rise:.1f}% leftover={leftover_ah:.3f}Ah "
                          f"coulombs={coulombs_counted:.3f}Ah "
                          f"delta_v={delta_v_mv:.1f}mV imbalance={imbalance_ah:.3f}Ah "
                          f"total_Q={total_q_ah:.3f}Ah")

                    con.execute("""
                        INSERT INTO pack_capacity_measurements
                            (id, device_id, cell_no, event_date,
                             charge_start_ts, charge_end_ts,
                             start_voltage, start_soc_ocv, end_soc,
                             coulombs_counted, remaining_q_ah, total_q_ah,
                             avg_temp, model_version)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        next_id, device_id, 0,
                        pd.to_datetime(segment.iloc[0]["ts"]).date(),
                        str(segment.iloc[0]["ts"]),
                        str(segment.iloc[-1]["ts"]),
                        start_voltage, start_soc_ocv, actual_end_soc,
                        coulombs_counted, leftover_ah, total_q_ah,
                        avg_temp, model_version
                    ])

                    next_id += 1
                    events_saved += 1
                    in_charge = False

    print(f"  {events_saved} event(s) saved.")
    return events_saved


def run_q_loss_calculations(con, cal_constants, cyc_constants, cal_version, cyc_version):
    print("\nRunning q_loss calculations...")

    cyclic_df = con.execute("""
        SELECT device_id, cell_no, temp_bucket, c_rate_bucket,
               avg_temp, avg_c_rate, total_q_throughput
        FROM mart_fortnight_cyclic_aging_bucket
    """).df()

    if not cyclic_df.empty:
        cyclic_df["q_loss"] = cyclic_df.apply(lambda r: compute_q_loss_cyclic(r, **cyc_constants), axis=1)
        con.register("cyclic_updates_view", cyclic_df)
        con.execute("""
            UPDATE mart_fortnight_cyclic_aging_bucket AS t
            SET q_loss = u.q_loss
            FROM cyclic_updates_view AS u
            WHERE t.device_id     = u.device_id
              AND t.cell_no       = u.cell_no
              AND t.temp_bucket   = u.temp_bucket
              AND t.c_rate_bucket = u.c_rate_bucket
        """)
        con.unregister("cyclic_updates_view")

        history_cyc = cyclic_df.copy()
        history_cyc["record_date"]      = datetime.today().date()
        history_cyc["aging_type"]       = "cyclic"
        history_cyc["avg_soc"]          = None
        history_cyc["total_rest_hours"] = None
        history_cyc["model_version"]    = cyc_version
        history_cyc["id"]               = range(_get_next_qloss_id(con), _get_next_qloss_id(con) + len(history_cyc))
        con.register("cyc_history_view", history_cyc[[
            "id", "device_id", "cell_no", "record_date", "aging_type",
            "q_loss", "avg_temp", "avg_c_rate", "total_q_throughput",
            "avg_soc", "total_rest_hours", "model_version"
        ]])
        con.execute("INSERT INTO q_loss_history SELECT * FROM cyc_history_view")
        con.unregister("cyc_history_view")
        print(f"  Cyclic: {len(cyclic_df)} rows.")

    cal_df = con.execute("""
        SELECT device_id, cell_no, soc_bucket, temp_bucket,
               avg_temp, avg_soc, total_rest_hours
        FROM mart_fortnight_calender_aging_bucket
    """).df()

    if not cal_df.empty:
        cal_df["q_loss"] = cal_df.apply(lambda r: compute_q_loss_calendar(r, **cal_constants), axis=1)
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

        history_cal = cal_df.copy()
        history_cal["record_date"]        = datetime.today().date()
        history_cal["aging_type"]         = "calendar"
        history_cal["avg_c_rate"]         = None
        history_cal["total_q_throughput"] = None
        history_cal["model_version"]      = cal_version
        history_cal["id"]                 = range(_get_next_qloss_id(con), _get_next_qloss_id(con) + len(history_cal))
        con.register("cal_history_view", history_cal[[
            "id", "device_id", "cell_no", "record_date", "aging_type",
            "q_loss", "avg_temp", "avg_c_rate", "total_q_throughput",
            "avg_soc", "total_rest_hours", "model_version"
        ]])
        con.execute("INSERT INTO q_loss_history SELECT * FROM cal_history_view")
        con.unregister("cal_history_view")
        print(f"  Calendar: {len(cal_df)} rows.")


def _next_version(con, table_type):
    tbl = "aging_constants_cyclic" if table_type == "cyclic" else "aging_constants_calendar"
    rows = con.execute(f"SELECT model_version FROM {tbl}").fetchall()
    nums = []
    for (v,) in rows:
        try:
            nums.append(int(v.lstrip("v")))
        except ValueError:
            pass
    return f"v{max(nums) + 1}" if nums else "v1"


def refit_constants(con):
    print("\nRefitting constants...")

    cyc_df = con.execute("""
        SELECT avg_temp, avg_c_rate, total_q_throughput, q_loss
        FROM q_loss_history
        WHERE aging_type = 'cyclic'
          AND q_loss IS NOT NULL AND avg_temp IS NOT NULL
          AND avg_c_rate IS NOT NULL AND total_q_throughput IS NOT NULL
    """).df()

    if len(cyc_df) >= 4:
        def cyclic_model(X, A, Ea, n, m):
            T = X[0] + 273.15
            return A * np.exp(-Ea / (8.314 * T)) * (X[1] ** n) * (X[2] ** m)
        try:
            popt, _ = curve_fit(
                cyclic_model,
                (cyc_df["avg_temp"].to_numpy(), cyc_df["avg_c_rate"].to_numpy(), cyc_df["total_q_throughput"].to_numpy()),
                cyc_df["q_loss"].to_numpy(),
                p0=[0.03, 16500, 0.3, 0.7], maxfev=10000
            )
            new_v = _next_version(con, "cyclic")
            con.execute("""
                INSERT INTO aging_constants_cyclic (model_version, A, Ea, n, m, fitted_on, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [new_v, *popt, datetime.now(), f"Refitted from {len(cyc_df)} rows"])
            print(f"  Cyclic -> {new_v}: A={popt[0]:.4e} Ea={popt[1]:.1f} n={popt[2]:.4f} m={popt[3]:.4f}")
        except Exception as e:
            print(f"  Cyclic refit failed: {e}")
    else:
        print("  Not enough cyclic history (need ≥4).")

    cal_df = con.execute("""
        SELECT avg_temp, avg_soc, total_rest_hours, q_loss
        FROM q_loss_history
        WHERE aging_type = 'calendar'
          AND q_loss IS NOT NULL AND avg_temp IS NOT NULL
          AND avg_soc IS NOT NULL AND total_rest_hours IS NOT NULL
    """).df()

    if len(cal_df) >= 4:
        def calendar_model(X, A_cal, B, C, D):
            T = X[0] + 273.15
            return A_cal * np.exp(B / T) * C * np.exp(D * (X[1] / 100)) * (X[2] ** 0.5)
        try:
            popt, _ = curve_fit(
                calendar_model,
                (cal_df["avg_temp"].to_numpy(), cal_df["avg_soc"].to_numpy(), cal_df["total_rest_hours"].to_numpy()),
                cal_df["q_loss"].to_numpy(),
                p0=[2.0e-8, 3485, 1.1, 2.0], maxfev=10000
            )
            new_v = _next_version(con, "calendar")
            con.execute("""
                INSERT INTO aging_constants_calendar (model_version, A_cal, B, C, D, fitted_on, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [new_v, *popt, datetime.now(), f"Refitted from {len(cal_df)} rows"])
            print(f"  Calendar -> {new_v}: A_cal={popt[0]:.4e} B={popt[1]:.1f} C={popt[2]:.4f} D={popt[3]:.4f}")
        except Exception as e:
            print(f"  Calendar refit failed: {e}")
    else:
        print("  Not enough calendar history (need ≥4).")

    print("Refit complete. Next run will automatically pick up the new version.")


def main():
    start_date, do_refit = parse_args()
    end_date = start_date + timedelta(days=NUM_DAYS)
    db_path  = os.path.join(DBT_PROJECT_DIR, DB_FILE)

    if do_refit:
        con = duckdb.connect(db_path)
        try:
            refit_constants(con)
        finally:
            con.close()
        return

    print(f"Processing {NUM_DAYS} days from {start_date.strftime('%Y-%m-%d')}...")

    for i in range(NUM_DAYS):
        date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        print(f"\n--- Day {i + 1}: {date_str} ---")
        run_dbt_command([
            "run", "--select",
            "stg_battery_raw_parquet_data",
            "inter_battery_variable_mapping",
            "mart_daily_cyclic_aging_bucket",
            "mart_daily_calendar_aging_bucket",
            "--vars", f'{{"process_date": "{date_str}"}}'
        ])

    print("\nRunning aggregation...")
    run_dbt_command([
        "run", "--select",
        "mart_fortnight_cyclic_aging_bucket",
        "mart_fortnight_calender_aging_bucket",
        "--vars",
        f'{{"start_date": "{start_date.strftime("%Y-%m-%d")}", '
        f'"end_date": "{end_date.strftime("%Y-%m-%d")}", '
        f'"process_date": "{start_date.strftime("%Y-%m-%d")}"}}'
    ])

    con = duckdb.connect(db_path)
    try:
        cal_constants, cyc_constants, cal_v, cyc_v = fetch_constants(con)
        ocv_voltages, ocv_socs = load_ocv_table(con)
        run_q_loss_calculations(con, cal_constants, cyc_constants, cal_v, cyc_v)
        events = detect_and_estimate_capacity(
            con,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            ocv_voltages, ocv_socs,
            cal_v
        )
        if events == 0:
            print("No charge events detected.")
    except BaseException as e:
        print(f"Error: {type(e).__name__}: {e}")
        traceback.print_exc()
    finally:
        con.close()

    print("\nDone.")


if __name__ == "__main__":
    main()