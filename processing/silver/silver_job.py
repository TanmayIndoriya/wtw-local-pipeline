from common.spark import get_spark

from processing.common.reader import read_bronze, read_silver
from processing.common.writer import write_silver, write_quarantine

from processing.silver.cleaner import clean
from processing.silver.validator import validate
from processing.silver.deduplicator import deduplicate
from processing.silver.enricher import enrich
from processing.silver.scd import apply_scd


def run(dataset: str):

    spark = get_spark()

    df = read_bronze(
        spark=spark,
        dataset=dataset,
    )

    df = clean(df)

    valid_df, invalid_df = validate(
        df,
        dataset,
    )

    if not invalid_df.rdd.isEmpty():
        write_quarantine(
            invalid_df,
            dataset,
        )

    valid_df = deduplicate(
        valid_df,
        dataset,
    )

    valid_df = enrich(
        valid_df,
        dataset,
    )

    if dataset == "customers":

        try:
            current_df = read_silver(
                spark=spark,
                dataset=dataset,
            )

        except Exception:
            current_df = None

        valid_df = apply_scd(
            incoming_df=valid_df,
            current_df=current_df,
        )

    write_silver(
        valid_df,
        dataset,
    )

    