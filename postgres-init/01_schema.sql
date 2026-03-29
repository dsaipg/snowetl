-- ELT Platform metadata schema
-- Snowflake target schema (mock)
CREATE SCHEMA IF NOT EXISTS snowflake_target;

-- Teams
CREATE TABLE teams (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Users
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    role VARCHAR(50) DEFAULT 'developer',
    team_id INTEGER REFERENCES teams(id),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Connections
CREATE TABLE connections (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    db_type VARCHAR(50) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    db_name VARCHAR(255) NOT NULL,
    secret_arn VARCHAR(500),
    -- local only fields (in AWS these come from Secrets Manager)
    db_user VARCHAR(255),
    db_password VARCHAR(255),
    -- Snowflake-specific fields
    snowflake_warehouse VARCHAR(255),
    snowflake_role VARCHAR(255),
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT true
);

-- Pipelines
CREATE TABLE pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    connection_id INTEGER REFERENCES connections(id),
    source_table VARCHAR(255) NOT NULL,
    watermark_column VARCHAR(255),
    merge_key_column VARCHAR(255),
    target_schema VARCHAR(255) DEFAULT 'snowflake_target',
    target_schema_name VARCHAR(255) DEFAULT 'ELT_STAGING',
    destination_connection_id INTEGER,
    target_table VARCHAR(255) NOT NULL,
    volume_alert_upper_pct INTEGER DEFAULT 200,
    volume_alert_lower_pct INTEGER DEFAULT 50,
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT true,
    dag_id VARCHAR(255)
);

-- Schedules
CREATE TABLE schedules (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipelines(id),
    cron_expression VARCHAR(100) DEFAULT '0 2 * * *',
    trigger_type VARCHAR(50) DEFAULT 'scheduled',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Pipeline dependencies
CREATE TABLE pipeline_dependencies (
    pipeline_id INTEGER REFERENCES pipelines(id),
    depends_on_id INTEGER REFERENCES pipelines(id),
    PRIMARY KEY (pipeline_id, depends_on_id)
);

-- Job runs
CREATE TABLE job_runs (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipelines(id),
    status VARCHAR(50) DEFAULT 'pending',
    phase_completed INTEGER DEFAULT 0,
    started_at TIMESTAMP DEFAULT NOW(),
    ended_at TIMESTAMP,
    rows_loaded INTEGER DEFAULT 0,
    s3_staging_path VARCHAR(500),
    error_message TEXT,
    volume_anomaly_flag BOOLEAN DEFAULT false,
    dag_run_id VARCHAR(255)
);

-- Schema cache
CREATE TABLE schemas (
    id SERIAL PRIMARY KEY,
    connection_id INTEGER REFERENCES connections(id),
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100),
    is_suggested_watermark BOOLEAN DEFAULT false,
    last_refreshed TIMESTAMP DEFAULT NOW()
);

-- Schema drift log
CREATE TABLE schema_drift_log (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipelines(id),
    detected_at TIMESTAMP DEFAULT NOW(),
    column_name VARCHAR(255),
    change_type VARCHAR(100),
    old_definition TEXT,
    new_definition TEXT,
    status VARCHAR(50) DEFAULT 'pending_review',
    resolved_at TIMESTAMP,
    resolved_by INTEGER
);

-- Seed data
INSERT INTO teams (name) VALUES ('Data Engineering'), ('Analytics'), ('Finance');

INSERT INTO connections (name, db_type, host, port, db_name, db_user, db_password, created_by)
VALUES ('Mock Source DB', 'postgresql', 'mock_source_db', 5432, 'sourcedb', 'sourceuser', 'sourcepassword', 1);

-- Seed some job run history for monitoring demo
INSERT INTO pipelines (name, connection_id, source_table, watermark_column, merge_key_column, target_table, created_by, dag_id)
VALUES 
    ('Load Customers', 1, 'customers', 'updated_at', 'id', 'customers', 1, 'pipeline_1'),
    ('Load Orders', 1, 'orders', 'updated_at', 'id', 'orders', 1, 'pipeline_2'),
    ('Load Products', 1, 'products', 'updated_at', 'id', 'products', 1, 'pipeline_3');

INSERT INTO schedules (pipeline_id, cron_expression) VALUES (1, '0 2 * * *'), (2, '0 3 * * *'), (3, '0 2 * * *');

INSERT INTO pipeline_dependencies (pipeline_id, depends_on_id) VALUES (2, 1);

-- Seed job run history (7 days for volume anomaly demo)
INSERT INTO job_runs (pipeline_id, status, started_at, ended_at, rows_loaded) VALUES
(1, 'complete', NOW() - INTERVAL '7 days', NOW() - INTERVAL '7 days' + INTERVAL '3 minutes', 980),
(1, 'complete', NOW() - INTERVAL '6 days', NOW() - INTERVAL '6 days' + INTERVAL '3 minutes', 1020),
(1, 'complete', NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days' + INTERVAL '4 minutes', 995),
(1, 'complete', NOW() - INTERVAL '4 days', NOW() - INTERVAL '4 days' + INTERVAL '3 minutes', 1050),
(1, 'complete', NOW() - INTERVAL '3 days', NOW() - INTERVAL '3 days' + INTERVAL '3 minutes', 1010),
(1, 'complete', NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days' + INTERVAL '4 minutes', 0, NULL),
(1, 'complete', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day' + INTERVAL '3 minutes', 1025),
(2, 'complete', NOW() - INTERVAL '7 days', NOW() - INTERVAL '7 days' + INTERVAL '5 minutes', 4980),
(2, 'complete', NOW() - INTERVAL '6 days', NOW() - INTERVAL '6 days' + INTERVAL '5 minutes', 5120),
(2, 'complete', NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days' + INTERVAL '4 minutes', 4950),
(2, 'failed', NOW() - INTERVAL '4 days', NOW() - INTERVAL '4 days' + INTERVAL '1 minutes', 0),
(2, 'complete', NOW() - INTERVAL '3 days', NOW() - INTERVAL '3 days' + INTERVAL '5 minutes', 5010),
(2, 'complete', NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days' + INTERVAL '6 minutes', 5200),
(2, 'complete', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day' + INTERVAL '5 minutes', 4890),
(3, 'complete', NOW() - INTERVAL '7 days', NOW() - INTERVAL '7 days' + INTERVAL '2 minutes', 450),
(3, 'complete', NOW() - INTERVAL '6 days', NOW() - INTERVAL '6 days' + INTERVAL '2 minutes', 460),
(3, 'complete', NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days' + INTERVAL '2 minutes', 445),
(3, 'complete', NOW() - INTERVAL '4 days', NOW() - INTERVAL '4 days' + INTERVAL '3 minutes', 470),
(3, 'complete', NOW() - INTERVAL '3 days', NOW() - INTERVAL '3 days' + INTERVAL '2 minutes', 455),
(3, 'complete', NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days' + INTERVAL '2 minutes', 462),
(3, 'complete', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day' + INTERVAL '2 minutes', 448);

-- Update volume anomaly flag for the 0-row run
UPDATE job_runs SET volume_anomaly_flag = true WHERE pipeline_id = 1 AND rows_loaded = 0;
UPDATE job_runs SET error_message = 'Connection timeout to source database' WHERE pipeline_id = 2 AND status = 'failed';
