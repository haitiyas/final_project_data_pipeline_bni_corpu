"""
dag_etl_fraud_labels.py
=====================
ETL pipeline: fraud_labels.csv → stg_fraud_labels → dim_fraud_labels

Task flow:
    create_tables  (SQLExecuteQueryOperator) : DDL stg_fraud_labels & dim_fraud_labels
    extract_load   (@task Python)            : baca CSV → stg_fraud_labels
    transform      (SQLExecuteQueryOperator) : stg_fraud_labels → dim_fraud_labels

Airflow Connection:
    conn_id = "postgres_etl"  (tipe: Postgres)
    Host: postgres-etl | Port: 5432 | DB: etl_db
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ─── Konstanta ────────────────────────────────────────────────────────────────
CONN_ID     = "postgres_etl" # <-- ganti dengan koneksi database yang sudah dibuat di airflow
SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "fraud_labels.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_fraud_labels (
    transaction_id TEXT,
    transaction_code TEXT,
    is_fraud BOOLEAN,
    fraud_type TEXT,
    fraud_score TEXT,
    flagged_at TEXT
);

CREATE TABLE IF NOT EXISTS dim_fraud_labels (
    transaction_id VARCHAR(20) PRIMARY KEY,
    transaction_code VARCHAR(30),
    is_fraud BOOLEAN,
    fraud_type VARCHAR(50),
    fraud_score NUMERIC(5,4),
    flagged_at TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_fraud_labels",
    description         = "ETL fraud_labels.csv → stg_fraud_labels → dim_fraud_labels",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "fraud_labels", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/fraud_labels"],
)
def dag_etl_fraud_labels():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_fraud_labels ──────────────────────────────────
    @task()
    def extract_load():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_fraud_labels"))
            c.commit()

        df.to_sql(
            name      = "stg_fraud_labels",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_fraud_labels → dim_fraud_labels ──────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform

dag_etl_fraud_labels()
