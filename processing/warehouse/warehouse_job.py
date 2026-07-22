from common.logger import get_logger

from processing.warehouse.postgres_loader import load_gold, load_silver

logger = get_logger(__name__)


def run(spark):

    logger.info("Loading Silver tables into PostgreSQL")

    load_silver(spark)

    logger.info("Loading Gold tables into PostgreSQL")

    load_gold(spark)

    logger.info("Warehouse loading completed")  