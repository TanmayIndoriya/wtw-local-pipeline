import json

from kafka import KafkaConsumer

from common.config import get_config
from common.logger import get_logger

logger = get_logger(__name__)


class KafkaConsumerClient:

    def __init__(self):

        config = get_config().get_kafka()["kafka"]

        self.bootstrap_servers = config["bootstrap_servers"]
        self.consumer_group = config["consumer_group"]
        self.auto_offset_reset = config["auto_offset_reset"]

    def consume(
        self,
        topic: str,
        max_records: int = 1000,
        timeout_ms: int = 5000,
    ) -> list[dict]:

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.consumer_group,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        records = []

        while len(records) < max_records:

            batches = consumer.poll(timeout_ms=timeout_ms)

            if not batches:
                break

            for _, messages in batches.items():

                for message in messages:

                    records.append(message.value)

                    if len(records) >= max_records:
                        break

        consumer.close()

        logger.info(
            "Consumed %s messages from topic '%s'",
            len(records),
            topic,
        )

        return records