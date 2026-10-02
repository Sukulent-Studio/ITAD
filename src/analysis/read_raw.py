"""Прочитать raw-объект из MinIO через DuckDB без создания таблиц."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb


def reader_for(filename: str) -> str:
    """Выбрать DuckDB table function по расширению исходного файла."""
    suffix = Path(filename).suffix.lower()
    readers = {
        ".csv": "read_csv_auto",
        ".json": "read_json_auto",
        ".jsonl": "read_json_auto",
        ".ndjson": "read_json_auto",
        ".parquet": "read_parquet",
    }
    try:
        return readers[suffix]
    except KeyError as error:
        message = f"Для формата {suffix or 'без расширения'} не задан DuckDB reader."
        raise ValueError(message) from error


def configure_minio(connection: duckdb.DuckDBPyConnection) -> None:
    """Настроить S3-совместимый доступ DuckDB к локальному SeaweedFS."""
    endpoint = os.getenv("S3_ENDPOINT", "localhost:8333")
    access_key = os.getenv("S3_ACCESS_KEY", "s3admin")
    secret_key = os.getenv("S3_SECRET_KEY", "s3admin123")

    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute(f"SET s3_endpoint = '{endpoint}'")
    connection.execute("SET s3_url_style = 'path'")
    connection.execute("SET s3_use_ssl = false")
    connection.execute(f"SET s3_access_key_id = '{access_key}'")
    connection.execute(f"SET s3_secret_access_key = '{secret_key}'")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, help="S3-путь к raw-объекту.")
    args = parser.parse_args()

    object_path = args.path
    reader = reader_for(object_path)

    connection = duckdb.connect()
    configure_minio(connection)
    rows = connection.execute(
        f"SELECT * FROM {reader}(?) LIMIT 5", [object_path]
    ).fetchall()
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
