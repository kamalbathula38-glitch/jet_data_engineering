"""
Airflow DAG: fetch new XKCD comics and refresh the analytics marts.

XKCD publishes Monday/Wednesday/Friday, but not at a fixed time of day, so
`wait_for_new_comic` polls the API every 30 minutes for up to 24 hours,
looking for a comic_num greater than what we already have. It only checks
for existence - it never caches the payload it sees - so `run_ingestion`
makes its own independent, fresh API call when it runs, which also picks up
anything XKCD tweaks on the comic shortly after publishing it.

Once a new comic is found, `run_ingestion` loads it into raw_xkcd and
`run_dbt_build` runs the dbt transforms and all defined data quality tests.

Setup:
    1. Install Airflow in its own environment - it is *not* in this project's
       requirements.txt, since its pinned dependency set tends to conflict
       with the ingestion/dbt stack. `pip install apache-airflow` separately.
    2. Set the JET_PROJECT_ROOT environment variable (in the Airflow worker's
       environment) to this repository's absolute path. It must contain a
       working .env (see .env.example) and have `dbt` on PATH.
    3. Copy or symlink this file into Airflow's configured dags_folder.
"""
from __future__ import annotations

from datetime import datetime
import os
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

PROJECT_ROOT = os.environ.get(
    "JET_PROJECT_ROOT",
    "/absolute/path/to/jet_data_engineering",
)
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt_project", "jet_analytics")

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _load_env() -> None:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def _new_comic_available() -> bool:
    from handlers.postgres import PostgresPublisher
    from handlers.xkcd import XKCDIngestion

    _load_env()
    watermark = PostgresPublisher().get_watermark("raw_xkcd", "comic_num")
    latest = XKCDIngestion().get_latest()
    return watermark is None or latest["comic_num"] > watermark


def _run_ingestion() -> None:
    from ingestion.base_ingestion import BaseIngestion
    from ingestion.config_parser import load_config

    _load_env()
    config_path = os.path.join(PROJECT_ROOT, "config", "ingestions", "xkcd.yaml")
    print(BaseIngestion(load_config(config_path)).run())


with DAG(
    dag_id="xkcd_ingestion_pipeline",
    description="Fetch new XKCD comics and refresh the analytics marts.",
    schedule_interval="0 0 * * MON,WED,FRI",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["xkcd_analytics"],
) as dag:

    wait_for_new_comic = PythonSensor(
        task_id="wait_for_new_comic",
        python_callable=_new_comic_available,
        poke_interval=30 * 60,  # 30 minutes
        timeout=24 * 60 * 60,  # poll for up to 24 hours
        mode="reschedule",  # free the worker slot between pokes
    )

    run_ingestion = PythonOperator(
        task_id="run_ingestion",
        python_callable=_run_ingestion,
    )

    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build",
    )

    wait_for_new_comic >> run_ingestion >> run_dbt_build
