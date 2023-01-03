
from utility import *
import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("analysis")

def analysis_1( df):
    '''

    :param df: primary_person data
    :return: result_1 file in results directory

    Return the No.Of crashes in which males died
    '''
    try:

        ### Replace null with NA
        df_replace_null= replace_null_with_char(df, columns=["DEATH_CNT","PRSN_GNDR_ID"], char="NA")


        ### Converting columns to lower case
        df_gender_lower = lower_convert(df_replace_null, columns=["PRSN_GNDR_ID"])

        ### Triming Columns
        df_gender_trim = remove_trim(df_gender_lower, columns=["PRSN_GNDR_ID"])

        ### Filtering male Death
        df_male_dead = (
            df_gender_trim.filter(
            (df_gender_trim.DEATH_CNT == 1) & (df_gender_trim.PRSN_GNDR_ID == 'male'))
        )

        ### Total crashes involving male death
        result = df_male_dead.groupBy("CRASH_ID").count().count()
        content = f"There are {result} crashes in which Males are killed"

        ### Writing result
        file_write(content, file_name= "result_1")

    except Exception as exp:
        logger.error("Error Occurred in the analysis_1() method. Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("analysis_1 completed, check results folder---> result_1")