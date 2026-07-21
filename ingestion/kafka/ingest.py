from common.logger import get_logger
from common.schemas import get_schema
from common.spark import get_spark
from common.utils import apply_schema

from ingestion.common.metadata import add_metadata
from ingestion.common.validation import (
    validate_not_empty,
    validate_schema,
)
from ingestion.kafka.consumer import KafkaConsumerClient
from ingestion.landing_writer import landing_writer


logger = get_logger(__name__)


class KafkaIngestor:

    def __init__(self):

        self.spark = get_spark()
        self.consumer = KafkaConsumerClient()

    def ingest(
        self,
        topic: str,
    ):

        messages = self.consumer.consume(topic)

        if not messages:
            logger.info(
                "No messages found for topic '%s'",
                topic,
            )
            return

        schema = get_schema(topic)

        # Create DataFrame from JSON events
        df = self.spark.createDataFrame(messages)

        # Cast business columns to expected types
        df = apply_schema(df, schema)

        validate_not_empty(df)
        validate_schema(df, schema)

        df = add_metadata(
            df,
            source="kafka",
            source_file=topic,
        )

        landing_writer.write(
            df=df,
            source="kafka",
            dataset=topic,
        )

        logger.info(
            "Kafka ingestion completed for '%s'",
            topic,
        )