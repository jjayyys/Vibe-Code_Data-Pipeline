import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Task callable
# ------------------------------------------------------------------

def ingest_taxi_data(**context) -> str:
    """
    PythonOperator callable that downloads NYC Yellow Taxi Trip Records,
    validates row count, and pushes the local file path to XCom.

    Returns:
        str: Local path where the raw CSV was saved.

    Raises:
        RuntimeError: If the file cannot be downloaded after all retries,
                        or if the downloaded file has fewer than 1 000 rows.
    """
    DOWNLOAD_URL = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    LOCAL_PATH = "/tmp/nyc_taxi_raw.csv"
    MIN_ROW_THRESHOLD = 1_000
    MAX_RETRIES = 3
    RETRY_BACKOFF_SECONDS = 5   # doubles each attempt: 5s → 10s → 20s
    CHUNK_SIZE = 8_192          # bytes per streaming chunk

    # --------------------------------------------------------------
    # 1. Stream-download with retry logic
    # --------------------------------------------------------------
    last_exception: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(
                "Download attempt %d/%d — URL: %s",
                attempt, MAX_RETRIES, DOWNLOAD_URL,
            )

            with requests.get(DOWNLOAD_URL, stream=True, timeout=60) as response:
                response.raise_for_status()

                total_bytes = 0
                with open(LOCAL_PATH, "wb") as fh:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:   # filter keep-alive empty chunks
                            fh.write(chunk)
                            total_bytes += len(chunk)

            log.info(
                "Download complete — %.2f MB written to %s",
                total_bytes / (1024 ** 2),
                LOCAL_PATH,
            )
            break   # success — exit retry loop

        except requests.exceptions.ConnectionError as exc:
            last_exception = exc
            wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning(
                "ConnectionError on attempt %d/%d: %s. Retrying in %ds …",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)

        except requests.exceptions.HTTPError as exc:
            raise RuntimeError(
                f"HTTP error while downloading taxi data: {exc}"
            ) from exc

    else:
        raise RuntimeError(
            f"Failed to download NYC taxi data after {MAX_RETRIES} attempts. "
            f"Last error: {last_exception}"
        )

    # --------------------------------------------------------------
    # 2. Row-count validation with pandas
    # --------------------------------------------------------------
    log.info("Validating downloaded file: %s", LOCAL_PATH)

    df = pd.read_csv(LOCAL_PATH, low_memory=False)
    row_count = len(df)

    log.info("Row count: %d", row_count)

    if row_count < MIN_ROW_THRESHOLD:
        raise RuntimeError(
            f"Validation failed — file contains only {row_count} rows "
            f"(minimum expected: {MIN_ROW_THRESHOLD}). "
            "The source may be empty or truncated."
        )

    log.info(
        "Validation passed — %d rows meet the minimum threshold of %d.",
        row_count, MIN_ROW_THRESHOLD,
    )

    # --------------------------------------------------------------
    # 3. Push file path to XCom for downstream tasks
    # --------------------------------------------------------------
    ti = context["ti"]
    ti.xcom_push(key="raw_file_path", value=LOCAL_PATH)
    log.info("Pushed XCom key='raw_file_path' → '%s'", LOCAL_PATH)

    return LOCAL_PATH   # also stored as the default XCom return value

# ------------------------------------------------------------------
# Placeholder for a downstream task (e.g. cleaning / transformation)
# ------------------------------------------------------------------

def transform_taxi_data(**context) -> None:
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="ingest_taxi_data", key="raw_file_path")
    log.info("Received raw file path from XCom: %s", raw_path)
    # TODO: cleaning, anomaly removal, star-schema loading …

    


# ------------------------------------------------------------------
# Default arguments applied to every task in the DAG
# ------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["de-alerts@yourcompany.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_ingestion",
    default_args=default_args,
    description="Ingest NYC Yellow Taxi Trip Records CSV and validate",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nyc-taxi", "ingestion"],
) as dag:
    
    # ------------------------------------------------------------------
    # Task definitions
    # ------------------------------------------------------------------
    start_task = EmptyOperator(task_id="start_task")
    
    ingest_task = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=ingest_taxi_data,
    )    
    
    # ------------------------------------------------------------------
    # Task dependencies
    # ------------------------------------------------------------------
    start_task >> ingest_task