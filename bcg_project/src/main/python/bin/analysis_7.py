from utility import *
import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("analysis")


def analysis_7(df, df2):

    '''

    :param df: Damages data
    :param df2: Units data
    :return: result_7 file in results directory

    Count of Distinct Crash IDs where No Damaged Property was observed, Damage Level
    is above 4, and car avails Insurance

    '''

    try:
        col1 = ['VEH_DMAG_SCL_1_ID', 'VEH_DMAG_SCL_2_ID']
        col2 = ['DAMAGED_PROPERTY']

        ### Replace null with NA
        df = replace_null_with_char(df, columns=col1, char="NA")
        df2 = replace_null_with_char(df2, columns=col2, char="NA")

        ### Converting columns to lower case
        df = lower_convert(df, columns=['FIN_RESP_TYPE_ID'])
        df2 = lower_convert(df2, columns=col2)

        ### Selecting Damage score greater than 4
        df = df.withColumn("damage_scr_cmb", F.concat_ws(" ", df.VEH_DMAG_SCL_1_ID, df.VEH_DMAG_SCL_2_ID))
        df = df.filter((df.damage_scr_cmb.contains("5")) |
                       (df.damage_scr_cmb.contains("6")) |
                       (df.damage_scr_cmb.contains("7")))

        ### Filtering car avils insurance
        df = df.filter(df.FIN_RESP_TYPE_ID.like("%insurance%"))

        ### Crashes with car insurance and has damage score > 4
        df = df.groupBy("CRASH_ID").count()

        ### Joining Dataframes
        cond = [df.CRASH_ID == df2.CRASH_ID]
        join_df = df.alias('df').join(df2.alias('df2'), cond, 'inner').select(df2.DAMAGED_PROPERTY, "df.*")

        join_df = join_df.filter(join_df.DAMAGED_PROPERTY.like("%no damage%"))

        result = join_df.groupBy("CRASH_ID").count().count()
        content = f"There are {result}-Count of Distinct Crash IDs where No Damaged Property was observed, Damage Level is above 4, and car avails Insurance"

        ### Writing result
        file_write(content, file_name="result_7")

    except Exception as exp:
        logger.error("Error Occurred in the analysis_7() method. Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("analysis_7 completed, check results folder---> result_7")