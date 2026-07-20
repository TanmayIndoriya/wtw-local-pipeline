from common.logger import get_logger
from common.schemas import get_schema

from ingestion.common.metadata import add_metadata
from ingestion.common.validation import (
    validate_not_empty,
    validate_schema,
)
from ingestion.landing_writer import landing_writer
from ingestion.mysql.extractor import MySQLExtractor

logger = get_logger(__name__)


class MySQLIngestor:

    def __init__(self):
        self.extractor = MySQLExtractor()

    def ingest(
        self,
        dataset: str,
        table: str,
    ):

        schema = get_schema(dataset)

        df = self.extractor.extract(table)

        validate_not_empty(df)
        validate_schema(df, schema)

        df = add_metadata(
            df,
            source="mysql",
            source_file=table,
        )

        landing_writer.write(
            df=df,
            source="mysql",
            dataset=dataset,
        )

        logger.info(
            "MySQL ingestion completed for '%s'",
            dataset,
        )