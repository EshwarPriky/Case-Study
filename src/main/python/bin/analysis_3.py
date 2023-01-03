
from utility import *
import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("analysis")


def analysis_3(df):

    '''

    :param df: primary_person data
    :return: result_3 file in results directory

    State with the highest No.Of Female crashes
    '''

    try:
        ### Replace null with NA
        df_remove_null= replace_null_with_char(df, columns=["DRVR_LIC_STATE_ID", "PRSN_GNDR_ID"], char="NA")


        ### Converting columns to lower case
        df_lower = lower_convert(df_remove_null, columns=["PRSN_GNDR_ID", "DRVR_LIC_STATE_ID"])


        ### Triming Columns
        df_trim = remove_trim(df_lower, columns=["PRSN_GNDR_ID", "DRVR_LIC_STATE_ID"])


        ### Filtering female crashes
        df_female_crash = df_trim.filter( F.col('PRSN_GNDR_ID') == "female").distinct()
        df_female_crash = remove_na(df_female_crash, columns=['DRVR_LIC_STATE_ID'])

        ### Top State for Female crashes
        df_top_state = (df_female_crash.groupBy(df_female_crash.DRVR_LIC_STATE_ID).count()
                        .orderBy(F.col('count').desc()).take(1))
        result = df_top_state[0][0]
        content = f" Top State with lot of female crashes are {result}"

        ### Writing result
        file_write(content, file_name="result_3")

    except Exception as exp:
        logger.error("Error Occurred in the analysis_3() method. Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("analysis_3 completed, check results folder---> result_3")