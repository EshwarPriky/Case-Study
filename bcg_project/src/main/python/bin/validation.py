import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("validations")

def get_spark_id(spark):
    try:
        logger.info(f"Spark Id -->{spark}")
    except Exception as exp:
        logger.error( "Error in the method -- get_spark_id(). Check the Stack Tree. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Spark Object is Validated")
