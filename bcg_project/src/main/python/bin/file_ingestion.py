import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("ingestion")

def load_file(spark, filename, filepath, header, inferSchema):
    try:

        df = ( spark.read.option("inferSchema", inferSchema).option("header", header)
                .csv(filepath))
        logger.info(f"{filename} ingestion completed")
        return df

    except Exception as exp:
        logger.error( "Error in the method -- load_file(). Check the Stack Tree. " + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"{filename} ingested successfully")
