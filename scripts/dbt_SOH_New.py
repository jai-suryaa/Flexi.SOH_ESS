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

DBT_PROJECT_DIR = "C:/EOL/SOH/Flexi.SOH_ESS"
DB_FILE = "dev.duckdb"

SEASONS = {
    "summer": [3, 4, 5, 6],
    "rainy": [7, 8, 9, 10],
    "winter": [11, 12, 1, 2]
}

def get_season_from_date(date):
    month = date.month
    year = date.year
    
    if month in [3, 4, 5, 6]:
        return "summer", year
    elif month in [7, 8, 9, 10]:
        return "rainy", year
    elif month in [11, 12]:
        return "winter", year
    elif month in [1, 2]:
        return "winter", year - 1
    
    raise ValueError(f"Invalid month: {month}")


def get_seasons_in_range(start_date, end_date):
    seasons_list = []
    current = start_date
    
    while current <= end_date:
        season, year = get_season_from_date(current)
        season_start, season_end = get_season_date_range(season, year)
        
        actual_start = max(season_start, start_date)
        actual_end = min(season_end, end_date)
        
        season_year = f"{season}_{year}"
        
        if not seasons_list or seasons_list[-1][2] != season_year:
            seasons_list.append((season, year, season_year, actual_start, actual_end))
        
        current = season_end + timedelta(days=1)
    
    return seasons_list


def parse_args():
    args = sys.argv[1:]
    do_refit = "--refit" in args
    args = [a for a in args if a != "--refit"]

    if args:
        try:
            start_date = datetime.strptime(args[0], "%Y-%m-%d")
            return start_date, do_refit
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            exit(1)

    date_input = input("Enter start date (YYYY-MM-DD): ").strip()
    try:
        start_date = datetime.strptime(date_input, "%Y-%m-%d")
        return start_date, do_refit
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        exit(1)


def get_season_date_range(season, year):
    months = SEASONS[season]
    
    if season == "winter":
        start_date = datetime(year, 11, 1)
        if (year + 1) % 4 == 0 and ((year + 1) % 100 != 0 or (year + 1) % 400 == 0):
            end_date = datetime(year + 1, 2, 29)
        else:
            end_date = datetime(year + 1, 2, 28)
    else:
        start_date = datetime(year, months[0], 1)
        
        last_month = months[-1]
        if last_month == 12:
            end_date = datetime(year, 12, 31)
        else:
            end_date = datetime(year, last_month + 1, 1) - timedelta(days=1)
    
    return start_date, end_date


def run_dbt_command(command_args):
    """Execute dbt command and handle errors"""
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
    """Fetch aging constants from database"""
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


def compute_q_loss_calendar(row, A_cal, B, C, D):
    """Calculate calendar aging q_loss"""
    T = row["avg_temp"] + 273.15
    return float(A_cal * math.exp(B / T) * C * math.exp(D * (row["avg_soc"] / 100)) * (row["total_rest_hours"] ** 0.5))


def compute_q_loss_cyclic(row, A, Ea, n, m):
    """Calculate cyclic aging q_loss"""
    T = row["avg_temp"] + 273.15
    return float(A * np.exp(-Ea / (8.314 * T)) * (row["avg_c_rate"] ** n) * (row["total_q_throughput"] ** m))


def _get_next_qloss_id(con):
    """Get next available ID for q_loss_history table"""
    return con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM q_loss_history").fetchone()[0]


def run_q_loss_calculations(con, cal_constants, cyc_constants, cal_version, cyc_version, season_year):
    """Calculate and store q_loss for the given season"""
    print("\nRunning q_loss calculations...")

    # Process cyclic aging
    cyclic_df = con.execute(f"""
        SELECT device_id, cell_no, temp_bucket, c_rate_bucket, season_year,
               avg_temp, avg_c_rate, total_q_throughput
        FROM mart_fortnight_cyclic_aging_bucket
        WHERE season_year = '{season_year}'
    """).df()

    if not cyclic_df.empty:
        cyclic_df["q_loss"] = cyclic_df.apply(lambda r: compute_q_loss_cyclic(r, **cyc_constants), axis=1)
        con.register("cyclic_updates_view", cyclic_df)
        con.execute(f"""
            UPDATE mart_fortnight_cyclic_aging_bucket AS t
            SET q_loss = u.q_loss
            FROM cyclic_updates_view AS u
            WHERE t.device_id     = u.device_id
              AND t.cell_no       = u.cell_no
              AND t.temp_bucket   = u.temp_bucket
              AND t.c_rate_bucket = u.c_rate_bucket
              AND t.season_year   = '{season_year}'
        """)
        con.unregister("cyclic_updates_view")

        # Store in history
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
        print(f"  Cyclic: {len(cyclic_df)} rows processed for {season_year}.")

    # Process calendar aging
    cal_df = con.execute(f"""
        SELECT device_id, cell_no, soc_bucket, temp_bucket, season_year,
               avg_temp, avg_soc, total_rest_hours
        FROM mart_fortnight_calender_aging_bucket
        WHERE season_year = '{season_year}'
    """).df()

    if not cal_df.empty:
        cal_df["q_loss"] = cal_df.apply(lambda r: compute_q_loss_calendar(r, **cal_constants), axis=1)
        con.register("calendar_updates_view", cal_df)
        con.execute(f"""
            UPDATE mart_fortnight_calender_aging_bucket AS t
            SET q_loss = u.q_loss
            FROM calendar_updates_view AS u
            WHERE t.device_id   = u.device_id
              AND t.cell_no     = u.cell_no
              AND t.soc_bucket  = u.soc_bucket
              AND t.temp_bucket = u.temp_bucket
              AND t.season_year = '{season_year}'
        """)
        con.unregister("calendar_updates_view")

        # Store in history
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
        print(f"  Calendar: {len(cal_df)} rows processed for {season_year}.")


def _next_version(con, table_type):
    """Generate next version number for constants"""
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
    """Refit aging constants based on accumulated history"""
    print("\nRefitting constants...")

    # Refit cyclic constants
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
        print(f"  Not enough cyclic history (need ≥4, have {len(cyc_df)}).")

    # Refit calendar constants
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
        print(f"  Not enough calendar history (need ≥4, have {len(cal_df)}).")

    print("Refit complete. Next run will automatically pick up the new version.")


def main():
    start_date, do_refit = parse_args()
    end_date = datetime.today()
    db_path = os.path.join(DBT_PROJECT_DIR, DB_FILE)

    # Handle refit mode
    if do_refit:
        con = duckdb.connect(db_path)
        try:
            refit_constants(con)
        finally:
            con.close()
        return

    # Get all seasons in the date range
    seasons = get_seasons_in_range(start_date, end_date)
    
    print(f"\n{'='*70}")
    print(f"Processing from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"{'='*70}")
    print(f"\nSeasons to process:")
    for season, year, season_year, s_start, s_end in seasons:
        days = (s_end - s_start).days + 1
        print(f"  • {season_year.upper()}: {s_start.strftime('%Y-%m-%d')} → {s_end.strftime('%Y-%m-%d')} ({days} days)")
    print(f"{'='*70}\n")

    # Process daily data for all days (once)
    print("STEP 1: Processing daily data for all days...")
    current_date = start_date
    day_count = 0
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        day_count += 1
        
        if day_count % 10 == 0:
            print(f"  Processed {day_count} days... (current: {date_str})")
        
        run_dbt_command([
            "run", "--select",
            "stg_battery_raw_parquet_data",
            "inter_battery_variable_mapping",
            "mart_daily_cyclic_aging_bucket",
            "mart_daily_calendar_aging_bucket",
            "--vars", f'{{"process_date": "{date_str}"}}',
            "--quiet"
        ])
        
        current_date += timedelta(days=1)
    
    print(f"  ✓ Completed processing {day_count} days\n")

    # Process each season separately
    print("STEP 2: Aggregating data by season...")
    con = duckdb.connect(db_path)
    
    try:
        cal_constants, cyc_constants, cal_v, cyc_v = fetch_constants(con)
        
        for season, year, season_year, s_start, s_end in seasons:
            print(f"\n  Processing {season_year.upper()}...")
            
            # Run seasonal aggregation for this specific season
            run_dbt_command([
                "run", "--select",
                "mart_fortnight_cyclic_aging_bucket",
                "mart_fortnight_calender_aging_bucket",
                "--vars",
                f'{{"start_date": "{s_start.strftime("%Y-%m-%d")}", '
                f'"end_date": "{(s_end + timedelta(days=1)).strftime("%Y-%m-%d")}", '
                f'"process_date": "{s_start.strftime("%Y-%m-%d")}"}}',
                "--quiet"
            ])
            
            # Calculate q_loss for this season
            run_q_loss_calculations(con, cal_constants, cyc_constants, cal_v, cyc_v, season_year)
            print(f"  ✓ {season_year} complete")
        
    except BaseException as e:
        print(f"Error: {type(e).__name__}: {e}")
        traceback.print_exc()
    finally:
        con.close()

    print(f"\n{'='*70}")
    print(f"✓ ALL PROCESSING COMPLETE")
    print(f"  Total days processed: {day_count}")
    print(f"  Seasons aggregated: {len(seasons)}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()