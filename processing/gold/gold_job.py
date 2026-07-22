from common.spark import get_spark

from processing.common.writer import write_gold

from processing.gold.customer_metrics import build as build_customer_metrics
from processing.gold.policy_metrics import build as build_policy_metrics
from processing.gold.claim_metrics import build as build_claim_metrics
from processing.gold.payment_metrics import build as build_payment_metrics

from common.logger import get_logger


def run():

    spark = get_spark()
    logger = get_logger(__name__)

    logger.info("Generating customer summary")

    customer_df = build_customer_metrics(spark)
    write_gold(customer_df, "customer_summary")

    logger.info("Generating policy summary")

    policy_df = build_policy_metrics(spark)
    write_gold(policy_df, "policy_summary")

    logger.info("Generating claim summary")

    claim_df = build_claim_metrics(spark)
    write_gold(claim_df, "claim_summary")

    logger.info("Generating payment summary")

    payment_df = build_payment_metrics(spark)
    write_gold(payment_df, "payment_summary")

    logger.info("Gold layer completed successfully")