"""Скачать небольшой HTTP-файл, распаковать архив и сохранить CSV в raw."""

from __future__ import annotations

import io
import logging
import zipfile
from typing import Any

import requests

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_BUCKET = "raw"
TABLE_SUFFIXES = {".csv", ".json", ".jsonl", ".ndjson", ".parquet"}


def load_to_raw(
    *,
    source_url: str,
    source_filename: str,
    dataset_slug: str,
    s3_conn_id: str,
    **context: Any,
) -> None:
    """Скачать HTTP-файл, распаковать и сохранить CSV в raw."""
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    if not s3_hook.check_for_bucket(bucket_name=RAW_BUCKET):
        s3_hook.create_bucket(bucket_name=RAW_BUCKET)
        logging.info("Created raw bucket: s3://%s", RAW_BUCKET)

    response = requests.get(source_url, timeout=60)
    response.raise_for_status()

    # Распаковываем архив и берём первый табличный файл.
    inner_name, inner_bytes = _extract_first_table_file(
        response.content, source_filename
    )
    inner_bytes = _ensure_utf8(inner_bytes, inner_name)

    # Ключ строим из имени файла внутри архива, а не из имени zip.
    base_name = inner_name.rsplit("/", 1)[-1]
    key = f"{dataset_slug}/ingested_on={context['ds']}/{base_name}"

    if s3_hook.check_for_key(key=key, bucket_name=RAW_BUCKET):
        logging.info(
            "Raw object already exists and will not be modified: s3://%s/%s",
            RAW_BUCKET,
            key,
        )
        return

    s3_hook.load_bytes(
        bytes_data=inner_bytes,
        key=key,
        bucket_name=RAW_BUCKET,
        replace=False,
    )
    logging.info(
        "Extracted %s from %s and uploaded to s3://%s/%s",
        inner_name,
        source_url,
        RAW_BUCKET,
        key,
    )


def _extract_first_table_file(zip_bytes: bytes, archive_name: str) -> tuple[str, bytes]:
    """Распаковать первый файл табличного формата из zip-архива."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [
            n
            for n in zf.namelist()
            if not n.endswith("/")
            and any(n.lower().endswith(s) for s in TABLE_SUFFIXES)
        ]
        if not names:
            raise ValueError(
                f"В архиве {archive_name} нет файлов табличного формата "
                f"({', '.join(sorted(TABLE_SUFFIXES))}). "
                f"Содержимое: {zf.namelist()}"
            )
        inner_name = names[0]
        return inner_name, zf.read(inner_name)


def _ensure_utf8(data: bytes, filename: str) -> bytes:
    """Перекодировать текстовый файл в UTF-8, если он в другой кодировке."""
    suffix = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if suffix not in {".csv", ".json", ".jsonl", ".ndjson"}:
        return data

    try:
        data.decode("utf-8")
        return data
    except UnicodeDecodeError:
        pass

    for enc in ("cp1251", "latin-1"):
        try:
            return data.decode(enc).encode("utf-8")
        except UnicodeDecodeError:
            continue

    raise ValueError(
        f"Не удалось определить кодировку файла {filename}. "
        "Попробуйте добавить нужную кодировку в _ensure_utf8."
    )