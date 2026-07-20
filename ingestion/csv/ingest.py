from pathlib import Path

from common.logger import get_logger
from common.schemas import get_schema

from ingestion.common.metadata import add_metadata
from ingestion.common.validation import (
    validate_not_empty,
    validate_schema,
)
from ingestion.csv.extractor import CSVExtractor
from ingestion.landing_writer import landing_writer

logger = get_logger(__name__)


class CSVIngestor:

    def __init__(self):
        self.extractor = CSVExtractor()

    def ingest(
        self,
        dataset: str,
        file_path: str | Path,
    ):

        schema = get_schema(dataset)

        df = self.extractor.extract(
            file_path=file_path,
            schema=schema,
        )

        validate_not_empty(df)
        validate_schema(df, schema)

        df = add_metadata(
            df,
            source="csv",
            source_file=str(file_path),
        )

        landing_writer.write(
            df=df,
            source="csv",
            dataset=dataset,
        )

        logger.info(
            "CSV ingestion completed for '%s'",
            dataset,
        )