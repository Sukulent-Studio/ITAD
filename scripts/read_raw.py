"""Прочитать raw-объект из SeaweedFS через DuckDB без создания таблиц."""

from __future__ import annotations

import argparse
import io
import os
import tempfile
import zipfile
from pathlib import Path

import boto3
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


def configure_seaweedfs(connection: duckdb.DuckDBPyConnection) -> None:
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


def _download_object(object_path: str) -> bytes:
    """Скачать объект из SeaweedFS через boto3 (SigV4 подписывается сам)."""
    endpoint = os.getenv("S3_ENDPOINT", "localhost:8333")
    access_key = os.getenv("S3_ACCESS_KEY", "s3admin")
    secret_key = os.getenv("S3_SECRET_KEY", "s3admin123")

    bucket_and_key = object_path.removeprefix("s3://")
    bucket, _, key = bucket_and_key.partition("/")

    client = boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def query_zip(connection, object_path):
    blob = _download_object(object_path)

    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        if not names:
            raise ValueError(f"Zip-архив {object_path} пуст")
        inner_name = names[0]
        inner_bytes = zf.read(inner_name)

    # Перекодируем байты в UTF-8, если это текстовый файл
    suffix = Path(inner_name).suffix.lower()
    if suffix in {".csv", ".json", ".jsonl", ".ndjson"}:
        try:
            inner_bytes.decode("utf-8")
        except UnicodeDecodeError:
            # Файл не в UTF-8 — пробуем cp1251
            inner_bytes = inner_bytes.decode("cp1251").encode("utf-8")

    reader = reader_for(inner_name)

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(inner_bytes)
        tmp_path = tmp.name

    try:
        rows = connection.execute(
            f"SELECT * FROM {reader}(?) LIMIT 5", [tmp_path]
        ).fetchall()
    finally:
        os.unlink(tmp_path)

    for row in rows:
        print(row)

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, help="S3-путь к raw-объекту.")
    args = parser.parse_args()

    object_path = args.path
    connection = duckdb.connect()
    configure_seaweedfs(connection)

    if object_path.lower().endswith(".zip"):
        query_zip(connection, object_path)
        return

    reader = reader_for(object_path)
    rows = connection.execute(
        f"SELECT * FROM {reader}(?) LIMIT 5", [object_path]
    ).fetchall()
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()