#SOH_Bucketted

import subprocess
from datetime import datetime, timedelta
import sys
import duckdb
import math
import numpy as np
import pandas as pd
import os
import traceback

NUM_DAYS = 15
DBT_PROJECT_DIR = "/home/tec-ubuntu/Jai-Codes/Flexi.SOH_bucketted"
DB_FILE = "dev.duckdb"

SOC_START_THRESHOLD   = 30.0
SOC_END_THRESHOLD     = 99.0
MIN_CHARGE_CURRENT    = 5.0
MIN_IDLE_DURATION_S   = 2400   # 40 minutes
MIN_CHARGE_DURATION_S = 2400   # 40 minutes
MIN_CHARGE_ROWS       = 2

MIN_EVENTS_FOR_SOH_UPDATE = 2

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
            return datetime.strptime(args[0], "%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            exit(1)

    date_input = input("Enter start date (YYYY-MM-DD): ").strip()
    try:
        return datetime.strptime(date_input, "%Y-%m-%d")
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
            shell=False,
            encoding="utf-8"
        )
        print(result.stdout)
        print(result.stderr)
        if result.returncode != 0:
            raise Exception("dbt failed")
    except Exception as e:
        print(f"Error running dbt: {' '.join(command_args)}\n{e}")
        exit(1)


def _next_version_for_device(con, table_type, device_id):
    tbl = "aging_constants_cyclic" if table_type == "cyclic" else "aging_constants_calendar"

    rows = con.execute(f"""
        SELECT model_version FROM {tbl}
        WHERE device_id = ? OR device_id IS NULL
    """, [device_id]).fetchall()

    nums = []
    for (v,) in rows:
        prefix = v.split("_")[0]
        try:
            nums.append(int(prefix.lstrip("v")))
        except ValueError:
            pass

    next_n = max(nums) + 1 if nums else 2
    return f"v{next_n}_{device_id}"


def fetch_constants_for_device(con, device_id, version=None):

    if version:
        cal_v = cyc_v = version
    else:
        cal_latest = con.execute("""
            SELECT model_version FROM aging_constants_calendar
            WHERE device_id = ?
            ORDER BY fitted_on DESC LIMIT 1
        """, [device_id]).fetchone()

        if cal_latest is None:
            cal_latest = con.execute("""
                SELECT model_version FROM aging_constants_calendar
                WHERE device_id IS NULL
                ORDER BY fitted_on DESC LIMIT 1
            """).fetchone()

        cyc_latest = con.execute("""
            SELECT model_version FROM aging_constants_cyclic
            WHERE device_id = ?
            ORDER BY fitted_on DESC LIMIT 1
        """, [device_id]).fetchone()

        if cyc_latest is None:
            cyc_latest = con.execute("""
                SELECT model_version FROM aging_constants_cyclic
                WHERE device_id IS NULL
                ORDER BY fitted_on DESC LIMIT 1
            """).fetchone()

        if cal_latest is None:
            raise ValueError(f"No calendar constants for device '{device_id}' and no global fallback.")
        if cyc_latest is None:
            raise ValueError(f"No cyclic constants for device '{device_id}' and no global fallback.")

        cal_v = cal_latest[0]
        cyc_v = cyc_latest[0]

        if cal_v != cyc_v:
            print(f"  [{device_id}] Warning: calendar version ({cal_v}) != cyclic version ({cyc_v}).")

    cal_row = con.execute("""
        SELECT A_cal, B, C, D FROM aging_constants_calendar
        WHERE model_version = ?
          AND (device_id = ? OR device_id IS NULL)
        ORDER BY CASE WHEN device_id = ? THEN 0 ELSE 1 END
        LIMIT 1
    """, [cal_v, device_id, device_id]).fetchone()

    if cal_row is None:
        raise ValueError(f"No calendar constants for version='{cal_v}', device='{device_id}'")

    cyc_row = con.execute("""
        SELECT A, Ea, n, m FROM aging_constants_cyclic
        WHERE model_version = ?
          AND (device_id = ? OR device_id IS NULL)
        ORDER BY CASE WHEN device_id = ? THEN 0 ELSE 1 END
        LIMIT 1
    """, [cyc_v, device_id, device_id]).fetchone()

    if cyc_row is None:
        raise ValueError(f"No cyclic constants for version='{cyc_v}', device='{device_id}'")

    print(f"  [{device_id}] Using constants: calendar={cal_v}, cyclic={cyc_v}")

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


def get_preceding_idle(seg, segments, idx):

    preceding_idles = [s for s in segments[:idx] if s["type"] == "idle"]
    if not preceding_idles:
        return None

    idle_seg = preceding_idles[-1]

    segments_between = segments[segments.index(idle_seg) + 1 : idx]
    if any(s["type"] == "charge" for s in segments_between):
        return None

    if idle_seg["duration_s"] < MIN_IDLE_DURATION_S:
        return None

    return idle_seg


def get_eligible_charge_segments(segments, device_id):

    eligible = []
    skipped_soc      = 0
    skipped_no_idle  = 0
    skipped_idle_short = 0
    skipped_too_short  = 0

    for idx, seg in enumerate(segments):
        if seg["type"] != "charge":
            continue

        charge_segment = seg["rows"]
        start_soc = float(charge_segment.iloc[0]["soc"])

        if start_soc >= SOC_START_THRESHOLD:
            skipped_soc += 1
            continue

        idle_seg = get_preceding_idle(seg, segments, idx)
        if idle_seg is None:
            preceding = [s for s in segments[:idx] if s["type"] == "idle"]
            if not preceding:
                skipped_no_idle += 1
            else:
                candidate = preceding[-1]
                between = segments[segments.index(candidate) + 1 : idx]
                if any(s["type"] == "charge" for s in between):
                    skipped_no_idle += 1
                else:
                    skipped_idle_short += 1
            continue

        charge_time = (
            pd.to_datetime(charge_segment.iloc[-1]["ts"]) -
            pd.to_datetime(charge_segment.iloc[0]["ts"])
        ).total_seconds()

        if charge_time < MIN_CHARGE_DURATION_S or len(charge_segment) < MIN_CHARGE_ROWS:
            skipped_too_short += 1
            continue

        eligible.append((idx, seg, idle_seg))

    print(
        f"[PRE-FILTER] {device_id} | "
        f"Eligible: {len(eligible)} | "
        f"Skipped(SOC too high): {skipped_soc} | "
        f"Skipped(no valid idle): {skipped_no_idle} | "
        f"Skipped(idle too short <40min): {skipped_idle_short} | "
        f"Skipped(charge too short): {skipped_too_short}"
    )

    return eligible


def detect_and_estimate_capacity(con, start_date, end_date, ocv_voltages, ocv_socs, model_version):
    con.execute("ALTER TABLE pack_capacity_measurements ADD COLUMN IF NOT EXISTS soh DOUBLE")
    con.execute("ALTER TABLE pack_capacity_measurements ADD COLUMN IF NOT EXISTS actual_capacity DOUBLE")

    soh_df = con.execute("""
        SELECT device_id, soh, actual_capacity
        FROM (
            SELECT device_id, soh, actual_capacity,
                   ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY event_date DESC) AS rn
            FROM pack_capacity_measurements
            WHERE soh IS NOT NULL AND actual_capacity IS NOT NULL
        ) t
        WHERE rn = 1
    """).df()

    latest_soh      = dict(zip(soh_df["device_id"], soh_df["soh"]))
    actual_capacity = dict(zip(soh_df["device_id"], soh_df["actual_capacity"]))

    print(f"[INIT] Loaded SOH for {len(latest_soh)} devices | Date range: {start_date} → {end_date}")

    raw_df = con.execute("""
        SELECT device_id, ts, v_min, current, soc, temp01, nominal_capacity,
               v1, v2, v3, v4, v5, v6, v7, v8,
               v9, v10, v11, v12, v13, v14, v15, v16
        FROM inter_battery_variable_mapping
        WHERE ts >= ?::timestamp AND ts < ?::timestamp
        ORDER BY device_id, ts
    """, [start_date, end_date]).df()

    if raw_df.empty:
        print("[WARN] No telemetry data found for the given date range. Exiting.")
        return 0

    print(f"[DATA] Fetched {len(raw_df)} rows across {raw_df['device_id'].nunique()} devices")

    cell_cols = [c for c in raw_df.columns if c.startswith("v") and c[1:].isdigit()]

    events_saved = 0
    next_id      = _get_next_capacity_id(con)
    batch_qs     = {}

    for device_id, group in raw_df.groupby("device_id"):
        group = group.sort_values("ts").reset_index(drop=True)

        device_soh     = latest_soh.get(device_id, 1.0)
        device_act_cap = actual_capacity.get(device_id, None)
        is_new_device  = device_act_cap is None

        print(f"\n[DEVICE] {device_id} | {'NEW' if is_new_device else 'KNOWN'} | SOH: {device_soh:.4f} | ActualCap: {device_act_cap}")

        group["seg_type"] = group["current"].apply(
            lambda c: "idle" if float(c) == 0.0 else ("charge" if float(c) >= MIN_CHARGE_CURRENT else "other")
        )
        group["seg_id"] = (group["seg_type"] != group["seg_type"].shift()).cumsum()

        segments = []
        for seg_id, seg in group.groupby("seg_id"):
            seg      = seg.reset_index(drop=True)
            start_ts = pd.to_datetime(seg.iloc[0]["ts"])
            end_ts   = pd.to_datetime(seg.iloc[-1]["ts"])
            segments.append({
                "seg_id":     seg_id,
                "type":       seg.iloc[0]["seg_type"],
                "start_ts":   start_ts,
                "end_ts":     end_ts,
                "duration_s": (end_ts - start_ts).total_seconds(),
                "rows":       seg,
            })

        charge_segs = [s for s in segments if s["type"] == "charge"]
        idle_segs   = [s for s in segments if s["type"] == "idle"]
        print(f"[SEGMENTS] {device_id} | Total: {len(segments)} | Charge: {len(charge_segs)} | Idle: {len(idle_segs)}")

        eligible = get_eligible_charge_segments(segments, device_id)

        if not eligible:
            print(f"[SKIP DEVICE] {device_id} | No eligible charge segments after pre-filter")
            continue

        for idx, seg, idle_seg in eligible:
            charge_segment = seg["rows"]

            idle_rows    = idle_seg["rows"]
            last_idle_row = idle_rows.iloc[-3]
            cell_v = pd.to_numeric(last_idle_row[cell_cols], errors="coerce").dropna()
            cell_v = cell_v[cell_v > 0]

            if not cell_v.empty:
                ocv_voltage = float(cell_v.min())
            elif pd.notna(last_idle_row.get("v_min")) and float(last_idle_row["v_min"]) > 0:
                ocv_voltage = float(last_idle_row["v_min"])
            else:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Could not extract valid OCV from last idle row")
                continue

            print(f"[OCV] {device_id} | seg {seg['seg_id']} | OCV: {ocv_voltage:.4f}V from last idle row at {last_idle_row['ts']}")

            ts_vals          = pd.to_datetime(charge_segment["ts"])
            dt_hours         = ts_vals.diff().dt.total_seconds().fillna(0) / 3600.0
            coulombs_counted = float((charge_segment["current"] * dt_hours).sum())

            if coulombs_counted <= 0:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Coulombs counted zero or negative: {coulombs_counted:.4f}")
                continue

            delta_v_mv = 0.0
            if cell_cols:
                for _, seg_row in charge_segment.iterrows():
                    cell_v = pd.to_numeric(seg_row[cell_cols], errors="coerce").dropna()
                    cell_v = cell_v[cell_v > 0]
                    if cell_v.empty:
                        continue
                    if abs(cell_v.max() - VREF_IMBALANCE) <= 0.01:
                        delta_v_mv = (cell_v.max() - cell_v.min()) * 1000.0
                        break

            if delta_v_mv == 0:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Imbalance threshold never hit, charge incomplete")
                continue

            imbalance_ah = get_imbalance_ah(max(0.0, delta_v_mv))
            nominal_cap  = float(charge_segment["nominal_capacity"].iloc[0])

            start_soc_ocv = voltage_to_soc(ocv_voltage, ocv_voltages, ocv_socs)
            if start_soc_ocv <= 0 or start_soc_ocv > SOC_START_THRESHOLD:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | OCV-derived SOC out of range: {start_soc_ocv:.2f}%")
                continue

            actual_end_soc = float(charge_segment.iloc[-1]["soc"])
            ocv_soc_rise   = actual_end_soc - start_soc_ocv
            if ocv_soc_rise < 10.0:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | SOC rise too small: {ocv_soc_rise:.2f}%")
                continue

            start_soc_frac = start_soc_ocv / 100.0
            base_cap       = device_act_cap if not is_new_device else nominal_cap
            leftover_ah    = start_soc_frac * device_soh * base_cap
            total_q_ah     = leftover_ah + coulombs_counted + imbalance_ah
            avg_temp       = float(charge_segment["temp01"].mean())

            print(
                f"[EVENT] {device_id} | seg {seg['seg_id']} | "
                f"StartSOC: {start_soc_ocv:.2f}% | EndSOC: {actual_end_soc:.2f}% | "
                f"Remaining: {leftover_ah:.4f} | Coulombs: {coulombs_counted:.4f}Ah | "
                f"Imbalance: {imbalance_ah:.4f}Ah | TotalQ: {total_q_ah:.4f}Ah"
            )

            con.execute("""
                INSERT INTO pack_capacity_measurements
                    (id, device_id, event_date,
                     charge_start_ts, charge_end_ts,
                     start_voltage, start_soc_ocv, end_soc, leftover_ah,
                     coulombs_counted, imbalance_q_ah, total_q_ah,
                     avg_temp, model_version, actual_capacity, soh)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                next_id, device_id,
                pd.to_datetime(charge_segment.iloc[0]["ts"]).date(),
                str(charge_segment.iloc[0]["ts"]),
                str(charge_segment.iloc[-3]["ts"]),
                ocv_voltage, start_soc_ocv, actual_end_soc, leftover_ah,
                coulombs_counted, imbalance_ah, total_q_ah,
                avg_temp, model_version,
                device_act_cap if not is_new_device else None,
                device_soh
            ])

            batch_qs.setdefault(device_id, []).append((total_q_ah, nominal_cap, ocv_soc_rise))
            next_id      += 1
            events_saved += 1

    print(f"\n[SUMMARY] Total charge events saved this run: {events_saved}")

    if batch_qs:
        for device_id, events in batch_qs.items():
            qs              = [e[0] for e in events]
            nominal         = events[0][1]
            weights         = [e[2] for e in events]
            prior_soh_value = latest_soh.get(device_id, None)

            print(f"\n[SOH UPDATE] {device_id} | Events this run: {len(events)} | Prior SOH: {prior_soh_value}")

            act_cap_row = con.execute("""
                SELECT actual_capacity FROM pack_capacity_measurements
                WHERE device_id = ? AND actual_capacity IS NOT NULL
                ORDER BY event_date ASC LIMIT 1
            """, [device_id]).fetchone()

            if prior_soh_value is None:
                if len(qs) < 2:
                    print(f"[SKIP SOH] {device_id} | New device but only {len(qs)} event(s) this run — need at least 2 to fix actual capacity")
                    continue

                dev_actual_cap = sum(q * w for q, w in zip(qs, weights)) / sum(weights)
                print(f"[NEW DEVICE] {device_id} | Fixing actual_capacity from {len(qs)} events → {dev_actual_cap:.4f}Ah")

                con.execute("""
                    UPDATE pack_capacity_measurements SET actual_capacity = ? WHERE device_id = ?
                """, [dev_actual_cap, device_id])
                new_soh = 1.0

            else:
                if act_cap_row is None or act_cap_row[0] is None:
                    print(f"[SKIP SOH] {device_id} | No baseline actual_capacity found in DB")
                    continue

                dev_actual_cap = float(act_cap_row[0])

                if len(qs) < MIN_EVENTS_FOR_SOH_UPDATE:
                    print(f"[SKIP SOH] {device_id} | Not enough events for SOH update: {len(qs)} < {MIN_EVENTS_FOR_SOH_UPDATE}")
                    con.execute("""
                        UPDATE pack_capacity_measurements
                        SET actual_capacity = ?
                        WHERE device_id = ?
                          AND event_date >= ?::date AND event_date < ?::date
                    """, [dev_actual_cap, device_id, start_date, end_date])
                    continue

                weighted_q = sum(q * w for q, w in zip(qs, weights)) / sum(weights)
                new_soh    = round(max(weighted_q / dev_actual_cap, 0.0), 6)
                new_soh    = min(new_soh, prior_soh_value)
                print(f"[KNOWN DEVICE] {device_id} | WeightedQ: {weighted_q:.4f}Ah | BaselineCap: {dev_actual_cap:.4f}Ah | NewSOH: {new_soh:.6f}")

            con.execute("""
                UPDATE pack_capacity_measurements
                SET actual_capacity = ?, soh = ?
                WHERE device_id = ?
                  AND event_date >= ?::date AND event_date < ?::date
            """, [dev_actual_cap, new_soh, device_id, start_date, end_date])

            print(f"[WRITTEN] {device_id} | actual_capacity: {dev_actual_cap:.4f}Ah | soh: {new_soh:.6f}")

    print(f"\n[DONE] detect_and_estimate_capacity complete | Events saved: {events_saved}")
    return events_saved


def run_q_loss_calculations(con):
    print("\nRunning q_loss calculations (per-device constants)...")

    cyclic_df = con.execute("""
        SELECT device_id, cell_no, temp_bucket, c_rate_bucket,
               avg_temp, avg_c_rate, total_q_throughput
        FROM mart_fortnight_cyclic_aging_bucket
    """).df()

    if not cyclic_df.empty:
        results_cyc = []
        for device_id, grp in cyclic_df.groupby("device_id"):
            try:
                _, cyc_constants, _, cyc_v = fetch_constants_for_device(con, device_id)
            except ValueError as e:
                print(f"  [{device_id}] Skipping cyclic q_loss: {e}")
                continue
            grp          = grp.copy()
            grp["q_loss"] = grp.apply(lambda r: compute_q_loss_cyclic(r, **cyc_constants), axis=1)
            grp["_cyc_v"] = cyc_v
            results_cyc.append(grp)

        if results_cyc:
            cyclic_df_out = pd.concat(results_cyc, ignore_index=True)
            con.register("cyclic_updates_view", cyclic_df_out)
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

            for device_id, grp in cyclic_df_out.groupby("device_id"):
                cyc_v = grp["_cyc_v"].iloc[0]
                hist  = grp.drop(columns=["_cyc_v"]).copy()
                hist["record_date"]      = datetime.today().date()
                hist["aging_type"]       = "cyclic"
                hist["avg_soc"]          = None
                hist["total_rest_hours"] = None
                hist["model_version"]    = cyc_v
                start_id  = _get_next_qloss_id(con)
                hist["id"] = range(start_id, start_id + len(hist))
                con.register("cyc_history_view", hist[[
                    "id", "device_id", "cell_no", "record_date", "aging_type",
                    "q_loss", "avg_temp", "avg_c_rate", "total_q_throughput",
                    "avg_soc", "total_rest_hours", "model_version"
                ]])
                con.execute("INSERT INTO q_loss_history SELECT * FROM cyc_history_view")
                con.unregister("cyc_history_view")

            print(f"  Cyclic: {len(cyclic_df_out)} rows across "
                  f"{cyclic_df_out['device_id'].nunique()} device(s).")

    cal_df = con.execute("""
        SELECT device_id, cell_no, soc_bucket, temp_bucket,
               avg_temp, avg_soc, total_rest_hours
        FROM mart_fortnight_calender_aging_bucket
    """).df()

    if not cal_df.empty:
        results_cal = []
        for device_id, grp in cal_df.groupby("device_id"):
            try:
                cal_constants, _, cal_v, _ = fetch_constants_for_device(con, device_id)
            except ValueError as e:
                print(f"  [{device_id}] Skipping calendar q_loss: {e}")
                continue
            grp           = grp.copy()
            grp["q_loss"] = grp.apply(lambda r: compute_q_loss_calendar(r, **cal_constants), axis=1)
            grp["_cal_v"] = cal_v
            results_cal.append(grp)

        if results_cal:
            cal_df_out = pd.concat(results_cal, ignore_index=True)
            con.register("calendar_updates_view", cal_df_out)
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

            for device_id, grp in cal_df_out.groupby("device_id"):
                cal_v = grp["_cal_v"].iloc[0]
                hist  = grp.drop(columns=["_cal_v"]).copy()
                hist["record_date"]        = datetime.today().date()
                hist["aging_type"]         = "calendar"
                hist["avg_c_rate"]         = None
                hist["total_q_throughput"] = None
                hist["model_version"]      = cal_v
                start_id  = _get_next_qloss_id(con)
                hist["id"] = range(start_id, start_id + len(hist))
                con.register("cal_history_view", hist[[
                    "id", "device_id", "cell_no", "record_date", "aging_type",
                    "q_loss", "avg_temp", "avg_c_rate", "total_q_throughput",
                    "avg_soc", "total_rest_hours", "model_version"
                ]])
                con.execute("INSERT INTO q_loss_history SELECT * FROM cal_history_view")
                con.unregister("cal_history_view")

            print(f"  Calendar: {len(cal_df_out)} rows across "
                  f"{cal_df_out['device_id'].nunique()} device(s).")


# def refit_constants(con):
#     print("\nRefitting constants from real capacity measurements (per-device)...")

#     device_ids = [r[0] for r in con.execute("""
#         SELECT DISTINCT device_id FROM pack_capacity_measurements WHERE device_id IS NOT NULL
#     """).fetchall()]

#     if not device_ids:
#         print("  No device data in pack_capacity_measurements. Nothing to refit.")
#         return

#     for device_id in device_ids:
#         print(f"\n  Device: {device_id}")

#         meas_df = con.execute("""
#             SELECT
#                 m.event_date,
#                 m.total_q_ah,
#                 m.avg_temp AS avg_temp,
#                 m.start_soc_ocv AS avg_soc,
#                 cyc.avg_c_rate,
#                 cyc.total_q_throughput,
#                 cal.total_rest_hours

#             FROM pack_capacity_measurements m

#             WHERE m.device_id = ?
#               AND cyc.avg_c_rate IS NOT NULL
#               AND cal.total_rest_hours IS NOT NULL
#               AND m.total_q_ah > 0

#         """, [device_id]).df()

#         if meas_df.empty:
#             print(f"    No usable measurements for {device_id}.")
#             continue

#         print(f"    {len(meas_df)} real capacity measurement(s) available for fitting.")

#         # --- Cyclic refit ---
#         cyc_mask = meas_df["avg_c_rate"].notna() & meas_df["total_q_throughput"].notna()
#         cyc_df   = meas_df[cyc_mask]

#         if len(cyc_df) >= 4:
#             def cyclic_model(X, A, Ea, n, m):
#                 T = X[0] + 273.15
#                 return A * np.exp(-Ea / (8.314 * T)) * (X[1] ** n) * (X[2] ** m)
#             try:
#                 popt, _ = curve_fit(
#                     cyclic_model,
#                     (cyc_df["avg_temp"].to_numpy(),
#                      cyc_df["avg_c_rate"].to_numpy(),
#                      cyc_df["total_q_throughput"].to_numpy()),
#                     cyc_df["total_q_ah"].to_numpy(),
#                     p0=[0.03, 16500, 0.3, 0.7],
#                     bounds=(
#                         [1e-6,  5000, 0.1, 0.3],
#                         [10.0, 80000, 2.0, 1.5],
#                     ),
#                     maxfev=10000
#                 )
#                 new_v = _next_version_for_device(con, "cyclic", device_id)
#                 con.execute("""
#                     INSERT INTO aging_constants_cyclic
#                         (model_version, A, Ea, n, m, device_id, fitted_on, description)
#                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#                 """, [new_v, float(popt[0]), float(popt[1]),
#                       float(popt[2]), float(popt[3]),
#                       device_id, datetime.now(),
#                       f"Refitted from {len(cyc_df)} real capacity measurements for device {device_id}"])
#                 print(f"    Cyclic  -> {new_v}: A={popt[0]:.4e} Ea={popt[1]:.1f} "
#                       f"n={popt[2]:.4f} m={popt[3]:.4f}")
#             except Exception as e:
#                 print(f"    Cyclic refit failed: {e}")
#         else:
#             print(f"    Not enough cyclic data (have {len(cyc_df)}, need >= 4).")

#         # --- Calendar refit ---
#         cal_mask = meas_df["avg_soc"].notna() & meas_df["total_rest_hours"].notna()
#         cal_df   = meas_df[cal_mask]

#         if len(cal_df) >= 4:
#             def calendar_model(X, A_cal, B, C, D):
#                 T = X[0] + 273.15
#                 return A_cal * np.exp(B / T) * C * np.exp(D * (X[1] / 100)) * (X[2] ** 0.5)
#             try:
#                 popt, _ = curve_fit(
#                     calendar_model,
#                     (cal_df["avg_temp"].to_numpy(),
#                      cal_df["avg_soc"].to_numpy(),
#                      cal_df["total_rest_hours"].to_numpy()),
#                     cal_df["total_q_ah"].to_numpy(),
#                     p0=[2.0e-8, 3485, 1.1, 2.0],
#                     bounds=(
#                         [1e-12, 1000, 0.1, 0.5],
#                         [1e-3,  8000, 5.0, 5.0],
#                     ),
#                     maxfev=10000
#                 )
#                 new_v = _next_version_for_device(con, "calendar", device_id)
#                 con.execute("""
#                     INSERT INTO aging_constants_calendar
#                         (model_version, A_cal, B, C, D, device_id, fitted_on, description)
#                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#                 """, [new_v, float(popt[0]), float(popt[1]),
#                       float(popt[2]), float(popt[3]),
#                       device_id, datetime.now(),
#                       f"Refitted from {len(cal_df)} real capacity measurements for device {device_id}"])
#                 print(f"    Calendar-> {new_v}: A_cal={popt[0]:.4e} B={popt[1]:.1f} "
#                       f"C={popt[2]:.4f} D={popt[3]:.4f}")
#             except Exception as e:
#                 print(f"    Calendar refit failed: {e}")
#         else:
#             print(f"    Not enough calendar data (have {len(cal_df)}, need >= 4).")

#     print("\nRefit complete. Next run will automatically pick up new per-device versions.")


def main():
    start_date = parse_args()
    end_date = start_date + timedelta(days=NUM_DAYS)
    db_path  = os.path.join(DBT_PROJECT_DIR, DB_FILE)

    con = duckdb.connect(db_path)
    con.execute("""
        DROP TABLE IF EXISTS inter_battery_variable_mapping
    """)
    con.close()

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

    con.execute("""
        CREATE TABLE IF NOT EXISTS aging_constants_calendar (
            model_version  VARCHAR PRIMARY KEY,
            A_cal          DOUBLE,
            B              DOUBLE,
            C              DOUBLE,
            D              DOUBLE,
            fitted_on      TIMESTAMP
        )
    """)

    con.execute("""
        INSERT INTO aging_constants_calendar
        VALUES (
            'v1',
            2.0e-8,
            3485,
            1.1,
            2,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (model_version) DO NOTHING
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS aging_constants_cyclic (
            model_version  VARCHAR PRIMARY KEY,
            A              DOUBLE,
            Ea             DOUBLE,
            n              DOUBLE,
            m              DOUBLE,
            fitted_on      TIMESTAMP
        )
    """)

    con.execute("""
        INSERT INTO aging_constants_cyclic
        VALUES (
            'v1',
            0.03,
            16500,
            0.3,
            0.7,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (model_version) DO NOTHING
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pack_capacity_measurements (
            id                INTEGER,
            device_id         VARCHAR,
            event_date        DATE,
            charge_start_ts   VARCHAR,
            charge_end_ts     VARCHAR,
            start_voltage     DOUBLE,
            start_soc_ocv     DOUBLE,
            end_soc            DOUBLE,
            leftover_ah        DOUBLE,
            coulombs_counted  DOUBLE,
            imbalance_q_ah    DOUBLE,
            total_q_ah        DOUBLE,
            avg_temp          DOUBLE,
            model_version     VARCHAR,
            actual_capacity   DOUBLE,
            soh               DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ocv_table (
            soc_percent  DOUBLE PRIMARY KEY,
            ocv_voltage  DOUBLE
        )
    """)

    count = con.execute(
        "SELECT COUNT(*) FROM ocv_table"
    ).fetchone()[0]

    if count == 0:
        con.execute("""
            INSERT INTO ocv_table VALUES
                (0,   2.973),
                (5,   3.183),
                (10,  3.202),
                (15,  3.222),
                (20,  3.246),
                (25,  3.262),
                (30,  3.278),
                (35,  3.284),
                (40,  3.285),
                (45,  3.285),
                (50,  3.288),
                (55,  3.289),
                (60,  3.291),
                (65,  3.312),
                (70,  3.325),
                (75,  3.326),
                (80,  3.326),
                (85,  3.326),
                (90,  3.327),
                (95,  3.329),
                (100, 3.339)
        """)

    try:
        ocv_voltages, ocv_socs = load_ocv_table(con)

        tag_version  = "unknown"
        # first_device = con.execute("""
        #     SELECT device_id FROM mart_fortnight_cyclic_aging_bucket LIMIT 1
        # """).fetchone()
        # if first_device:
        #     try:
        #         _, _, cal_v, _ = fetch_constants_for_device(con, first_device[0])
        #         tag_version = cal_v
        #     except ValueError:
        #         pass

        # run_q_loss_calculations(con)
        # events = detect_and_estimate_capacity(
        #     con,
        #     start_date.strftime("%Y-%m-%d"),
        #     end_date.strftime("%Y-%m-%d"),
        #     ocv_voltages, ocv_socs,
        #     tag_version
        # )
        # if events == 0:
        #     print("No charge events detected.")
    except BaseException as e:
        print(f"Error: {type(e).__name__}: {e}")
        traceback.print_exc()
    finally:
        con.close()

    print("\nDone.")


if __name__ == "__main__":
    main()