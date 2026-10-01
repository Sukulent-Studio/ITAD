"""DAG ЛР № 2: дождаться HTTP-источника и загрузить его в raw."""

from datetime import datetime
from urllib.parse import urlsplit

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from ingestion.load_raw import load_to_raw

# Публичные параметры конкретного учебного источника.
# Host URL должен совпадать с AIRFLOW_CONN_SOURCE_HTTP_CONN в .env.
SOURCE_URL = "https://archive.ics.uci.edu/static/public/560/seoul+bike+sharing+demand.zip"
SOURCE_FILENAME = "seoul+bike+sharing+demand.zip"
DATASET_SLUG = "seoul_bike"
HTTP_CONN_ID = "source_http_conn"
S3_CONN_ID = "s3_conn"

parsed_url = urlsplit(SOURCE_URL)
endpoint = parsed_url.path or "/"
if parsed_url.query:
    endpoint = f"{endpoint}?{parsed_url.query}"

with DAG(
    dag_id="ingest_raw",
    description="Проверяет HTTP-источник и записывает сырые данные в raw-слой MinIO.",
    start_date=datetime(2026, 9, 16),
    schedule=None,
    catchup=True,
    tags=["raw"],
) as dag:
    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id=HTTP_CONN_ID,
        endpoint=endpoint,
        method="HEAD",
        poke_interval=60,
        timeout=600,
    )

    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_url": SOURCE_URL,
            "source_filename": SOURCE_FILENAME,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": S3_CONN_ID,
        },
    )

    wait_for_primary_source >> load_to_raw_task
