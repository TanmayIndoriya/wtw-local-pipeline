from common.constants import MYSQL_TABLES
from ingestion.mysql.ingest import MySQLIngestor
from common.constants import KAFKA_TOPICS
from ingestion.kafka.ingest import KafkaIngestor


def ingest_mysql():
    ingestor = MySQLIngestor()

    for dataset, table in MYSQL_TABLES.items():
        ingestor.ingest(dataset, table)


def ingest_kafka():
    ingestor = KafkaIngestor()

    for topic in KAFKA_TOPICS:
        ingestor.ingest(topic)