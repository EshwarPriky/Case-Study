from utility import *
from pyspark.sql.window import Window
import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("analysis")



def analysis_5(df, df2):

    '''

    :param df: primary_person data
    :param df2: Units data
    :return: result_5 file in results directory

    Return the top Ethnic group across each body style

    '''

    try:
        ### Join DataFrame
        cond = [(df.CRASH_ID == df2.CRASH_ID) & (df.UNIT_NBR == df2.UNIT_NBR)]
        joined_df = df.join(df2, cond, 'inner')

        ### Replace null with NA
        joined_df_1 = replace_null_with_char(joined_df, columns=['VEH_BODY_STYL_ID'], char='NA')

        ### filter NA, Unknown values
        joined_df_2 = joined_df_1.filter( ~ (joined_df_1.VEH_BODY_STYL_ID.isin(['NA', 'UNKNOWN'])))


        ### Top Ethnic group for each body style
        joined_df_3 = joined_df_2.groupBy(joined_df_2.VEH_BODY_STYL_ID, joined_df_2.PRSN_ETHNICITY_ID).count()
        win = Window.partitionBy(joined_df_3.VEH_BODY_STYL_ID).orderBy(F.col('count').desc())

        result = (joined_df_3.withColumn("row_num", F.row_number().over(win)).filter(F.col("row_num") == 1)
               .select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID'))

        ### Writing result
        write_csv(result, file_name="result_5")

    except Exception as exp:
        logger.error("Error Occurred in the analysis_5() method. Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("analysis_5 completed, check results folder---> result_5")


