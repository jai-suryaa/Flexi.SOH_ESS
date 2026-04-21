INSERT INTO ocv_table VALUES (2.973, 0), (3.183, 5), (3.202, 10), (3.222, 15), (3.246, 20), (3.262, 25), (3.278, 30), (3.284, 35), (3.285, 40), (3.285, 45), (3.288, 50), (3.289, 55), (3.291, 60), (3.312, 65), (3.325, 70), (3.326, 75), (3.326, 80), (3.326, 85), (3.327, 90), (3.329, 95), (3.339, 100);

CREATE TABLE ocv_table (
    ocv_voltage  DOUBLE PRIMARY KEY,
    soc_percent  DOUBLE
);

CREATE TABLE IF NOT EXISTS aging_constants_calendar (
    model_version  VARCHAR PRIMARY KEY,
    A_cal          DOUBLE,
    B              DOUBLE,
    C              DOUBLE,
    D              DOUBLE,
    fitted_on      TIMESTAMP,
    description    VARCHAR
);

CREATE TABLE IF NOT EXISTS aging_constants_cyclic (
    model_version  VARCHAR PRIMARY KEY,
    A              DOUBLE,
    Ea             DOUBLE,
    n              DOUBLE,
    m              DOUBLE,
    fitted_on      TIMESTAMP,
    description    VARCHAR
);

INSERT INTO aging_constants_calendar VALUES (
    'v1', 2.0e-8, 3485, 1.1, 2, NOW(), 'Initial default constants'
);

INSERT INTO aging_constants_cyclic VALUES (
    'v1', 0.03, 16500, 0.3, 0.7, NOW(), 'Initial default constants'
);

CREATE TABLE pack_capacity_measurements (
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
);

CREATE TABLE q_loss_history (
    id              INTEGER PRIMARY KEY,
    device_id       VARCHAR,
    cell_no         VARCHAR,
    record_date     DATE,
    aging_type      VARCHAR,   
    q_loss          DOUBLE,
    avg_temp        DOUBLE,
    avg_c_rate      DOUBLE,    
    total_q_throughput DOUBLE, 
    avg_soc         DOUBLE,
    total_rest_hours DOUBLE,
    model_version   VARCHAR
);