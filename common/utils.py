from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, sha2

def current_timestamp() -> datetime:
    return datetime.now(timezone.utc)


def current_timestamp_str(
    fmt: str = "%Y-%m-%d %H:%M:%S"
) -> str:

    return current_timestamp().strftime(fmt)


def generate_uuid() -> str:
    return str(uuid.uuid4())


def calculate_hash(value: str) -> str:

    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def calculate_record_hash(values: Iterable[Any]) -> str:

    record = "|".join("" if v is None else str(v) for v in values)
    return calculate_hash(record)


def add_record_hash(
    df: DataFrame,
    columns: List[str],
    hash_column: str = "_record_hash",
) -> DataFrame:

    return df.withColumn(
        hash_column,
        sha2(
            concat_ws(
                "||",
                *[col(column) for column in columns],
            ),
            256,
        ),
    )


def read_json(path: Path) -> Dict[str, Any]:

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def write_json(
    path: Path,
    data: Dict[str, Any],
    indent: int = 4,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            indent=indent,
            ensure_ascii=False,
        )


def file_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def is_empty_dataframe(df: DataFrame) -> bool:
    return df.rdd.isEmpty()


def validate_required_columns(
    df: DataFrame,
    required_columns: List[str],
) -> None:

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )


def chunk_list(
    items: List[Any],
    size: int,
) -> List[List[Any]]:

    return [
        items[index:index + size]
        for index in range(
            0,
            len(items),
            size,
        )
    ]