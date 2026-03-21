import logging
import time
import requests
import pandas as pd
from datetime import timedelta, datetime
from airflow.exceptions import AirflowException

def ingest_taxi_data(**context) -> str:
    """
    Downloads NYC Yellow Taxi Trip Records CSV (capped at ROW_LIMIT rows),
    validates row count, and pushes the file path to XCom for downstream tasks.

    Args:
        **context: Airflow context dictionary injected by the PythonOperator.

    Returns:
        str: Local file path of the downloaded CSV.

    Raises:
        AirflowException: If all retry attempts fail or row count validation fails.
    """
    log = logging.getLogger(__name__)
    DOWNLOAD_URL = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    LOCAL_PATH = "/tmp/nyc_taxi_raw.csv"
    ROW_LIMIT = 1000        # Maximum data rows to retain (excludes header)
    MIN_ROW_COUNT = 100     # Sanity-check floor — fail if source returns fewer
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5
    CHUNK_SIZE = 8192       # 8 KB chunks for memory-efficient streaming

    # ------------------------------------------------------------------ #
    # 1. Stream-download with retry logic                                #
    # ------------------------------------------------------------------ #
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Download attempt %d/%d — URL: %s", attempt, MAX_RETRIES, DOWNLOAD_URL)

            response = requests.get(
                DOWNLOAD_URL,
                stream=True,
                timeout=(10, 60),  # (connect timeout, read timeout) in seconds
                headers={"User-Agent": "AirflowNYCTaxiPipeline/1.0"},
            )
            response.raise_for_status()

            # ---------------------------------------------------------- #
            # Stream line-by-line and stop once ROW_LIMIT lines written  #
            # ---------------------------------------------------------- #
            rows_written = 0          # counts data rows (header excluded)
            header_written = False

            with open(LOCAL_PATH, "wb") as fh:
                for raw_line in response.iter_lines():
                    if not raw_line:          # skip keep-alive empty lines
                        continue

                    line_bytes = raw_line + b"\n"

                    if not header_written:    # always write the header first
                        fh.write(line_bytes)
                        header_written = True
                        continue

                    if rows_written >= ROW_LIMIT:
                        log.info(
                            "Row limit of %d reached — closing stream early.",
                            ROW_LIMIT,
                        )
                        break

                    fh.write(line_bytes)
                    rows_written += 1

            log.info(
                "Download complete — %d data rows written to %s",
                rows_written,
                LOCAL_PATH,
            )
            break  # Success — exit retry loop

        except requests.exceptions.ConnectionError as exc:
            last_exception = exc
            log.warning(
                "Connection error on attempt %d/%d: %s", attempt, MAX_RETRIES, exc
            )
        except requests.exceptions.Timeout as exc:
            last_exception = exc
            log.warning(
                "Request timed out on attempt %d/%d: %s", attempt, MAX_RETRIES, exc
            )
        except requests.exceptions.HTTPError as exc:
            raise AirflowException(
                f"HTTP error — will not retry: {exc}"
            ) from exc

        if attempt < MAX_RETRIES:
            log.info("Retrying in %d seconds…", RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)
    else:
        raise AirflowException(
            f"All {MAX_RETRIES} download attempts failed. "
            f"Last error: {last_exception}"
        ) from last_exception

    # ------------------------------------------------------------------ #
    # 2. Row-count validation with pandas                                #
    # ------------------------------------------------------------------ #
    log.info("Validating downloaded file: %s", LOCAL_PATH)

    try:
        df_index = pd.read_csv(LOCAL_PATH, usecols=[0], low_memory=True)
        row_count = len(df_index)
    except Exception as exc:
        raise AirflowException(
            f"Failed to read CSV for validation: {exc}"
        ) from exc

    log.info("Row count (excluding header): %d", row_count)

    if row_count < MIN_ROW_COUNT:
        raise AirflowException(
            f"Validation failed — expected at least {MIN_ROW_COUNT} rows, "
            f"but found {row_count}. The file may be truncated or empty."
        )

    if row_count > ROW_LIMIT:
        raise AirflowException(
            f"Validation failed — file contains {row_count} rows, "
            f"which exceeds the configured ROW_LIMIT of {ROW_LIMIT}."
        )

    log.info(
        "Row count validation passed (%d rows, within [%d, %d]).",
        row_count, MIN_ROW_COUNT, ROW_LIMIT,
    )

    # ------------------------------------------------------------------ #
    # 3. Push file path and metadata to XCom for downstream tasks        #
    # ------------------------------------------------------------------ #
    context["ti"].xcom_push(key="raw_file_path", value=LOCAL_PATH)
    context["ti"].xcom_push(key="raw_row_count", value=row_count)
    log.info("Pushed file path and row count to XCom.")

    return LOCAL_PATH