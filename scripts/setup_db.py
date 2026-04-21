import duckdb
import os

DBT_PROJECT_DIR = "C:/EOL/dbt_SOH"
DB_FILE = "dev.duckdb"
CSV_PATH = "C:/EOL/a01_device_mapping.csv"

db_path = os.path.join(DBT_PROJECT_DIR, DB_FILE)
con = duckdb.connect(db_path)

try:
    con.execute("""
        CREATE TABLE IF NOT EXISTS ocv_table (
            soc_percent  DOUBLE PRIMARY KEY,
            ocv_voltage  DOUBLE
        )
    """)

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

    con.execute("""
        CREATE TABLE IF NOT EXISTS aging_constants_calendar (
            model_version  VARCHAR PRIMARY KEY,
            A_cal          DOUBLE,
            B              DOUBLE,
            C              DOUBLE,
            D              DOUBLE,
            fitted_on      TIMESTAMP,
            description    VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS aging_constants_cyclic (
            model_version  VARCHAR PRIMARY KEY,
            A              DOUBLE,
            Ea             DOUBLE,
            n              DOUBLE,
            m              DOUBLE,
            fitted_on      TIMESTAMP,
            description    VARCHAR
        )
    """)

    con.execute("""
        INSERT INTO aging_constants_calendar VALUES
            ('v1', 2.0e-8, 3485, 1.1, 2, NOW(), 'Initial default constants')
    """)

    con.execute("""
        INSERT INTO aging_constants_cyclic VALUES
            ('v1', 0.03, 16500, 0.3, 0.7, NOW(), 'Initial default constants')
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pack_capacity_measurements (
            id                  INTEGER PRIMARY KEY,
            device_id           VARCHAR,
            cell_no             VARCHAR,
            event_date          DATE,
            charge_start_ts     TIMESTAMP,
            charge_end_ts       TIMESTAMP,
            start_voltage       DOUBLE,
            start_soc_ocv       DOUBLE,
            end_soc             DOUBLE,
            coulombs_counted    DOUBLE,
            remaining_q_ah      DOUBLE,
            total_q_ah          DOUBLE,
            avg_temp            DOUBLE,
            model_version       VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS q_loss_history (
            id                  INTEGER PRIMARY KEY,
            device_id           VARCHAR,
            cell_no             VARCHAR,
            record_date         DATE,
            aging_type          VARCHAR,
            q_loss              DOUBLE,
            avg_temp            DOUBLE,
            avg_c_rate          DOUBLE,
            total_q_throughput  DOUBLE,
            avg_soc             DOUBLE,
            total_rest_hours    DOUBLE,
            model_version       VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS device_pack_mapping (
            unique_id  VARCHAR PRIMARY KEY,
            pack_id    VARCHAR
        )
    """)

    con.execute(f"""
        INSERT OR IGNORE INTO device_pack_mapping
        SELECT unique_id, pack_id
        FROM read_csv_auto('{CSV_PATH}')
    """)

    count = con.execute("SELECT COUNT(*) FROM device_pack_mapping").fetchone()[0]
    print(f"  device_pack_mapping: {count} A01 device(s) loaded.")

    print("\nSetup complete.")

except Exception as e:
    print(f"Setup failed: {e}")
    raise

finally:
    con.close()