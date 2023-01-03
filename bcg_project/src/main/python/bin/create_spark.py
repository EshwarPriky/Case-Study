from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("create_objects")

def create_spark_object (envn, appname):
    try:
        logger.info(f"create_spark() is started. {envn}  environment is used")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appname) \
            .getOrCreate()
    except Exception as exp:
        logger.error("Error in the method -- create_spark_object(). Check the Stack Tree. " + str(exp), exc_info= True)
        raise
    else:
        logger.info("Spark Object is created...")
    return spark