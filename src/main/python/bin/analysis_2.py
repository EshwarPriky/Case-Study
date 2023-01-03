
from utility import *
import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("analysis")


def analysis_2(df):
    '''

    :param df: Units data
    :return: result_2 file in results directory

    Considering both MotorCycle and Police-MotorCycle for the condition

    No.of Two-wheelers involved in crashes
    '''

    try:
        ### Replace null with NA
        df_replace_null = replace_null_with_char(df, columns=["VEH_BODY_STYL_ID"], char="NA")

        ### Converting columns to lower case
        df_lower = lower_convert(df_replace_null, columns=["VEH_BODY_STYL_ID"])


        ### Crashes involving motorcycle
        df_motocycle_crash = df_lower.filter(F.col('VEH_BODY_STYL_ID').contains("motorcycle"))
        result = df_motocycle_crash.groupBy("CRASH_ID", "UNIT_NBR").count().count()
        content = f"The No of MotorCycle involved in crashes are {result}"

        ### Writing result
        file_write(content, file_name="result_2")

    except Exception as exp:
        logger.error("Error Occurred in the analysis_2() method. Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("analysis_2 completed, check results folder---> result_2")