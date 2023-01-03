from utility import *
import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("analysis")




def analysis_6(df, df2):

    '''

    :param df: primary_person data
    :param df2: Units data
    :return: result_6 file in results directory

    Return the top 5 Zip code with Alcohol has cause of crash

    '''

    try:
        col1 = ['DRVR_ZIP', 'PRSN_ALC_RSLT_ID']
        col2 = ['CONTRIB_FACTR_P1_ID', 'CONTRIB_FACTR_1_ID', 'CONTRIB_FACTR_2_ID']

        ### Replace null with NA
        df = replace_null_with_char(df, columns=col1, char="NA")
        df2 = replace_null_with_char(df2, columns=col2, char="NA")

        ### Triming Columns
        df = remove_trim(df, columns=col1)
        df2 = remove_trim(df2, columns=col2)

        ### Join DataFrame
        cond = [(df.CRASH_ID == df2.CRASH_ID) & (df.UNIT_NBR == df2.UNIT_NBR)]
        joined_df = (df.join(df2, cond, 'inner').
                     select(df.DRVR_ZIP, df.PRSN_ALC_RSLT_ID, df2.CONTRIB_FACTR_P1_ID,
                            df2.CONTRIB_FACTR_1_ID, df2.CONTRIB_FACTR_2_ID)
                     )
        ### alcohol contribution selection
        joined_df = joined_df.withColumn("all_contrib",
                                         F.concat_ws(" ", joined_df.CONTRIB_FACTR_1_ID, joined_df.CONTRIB_FACTR_2_ID,
                                                     joined_df.CONTRIB_FACTR_P1_ID))

        joined_df = lower_convert(joined_df, columns=['all_contrib'])
        joined_df = lower_convert(joined_df, columns=['DRVR_ZIP'])

        df_contain_alcohol = joined_df.filter(joined_df.all_contrib.contains("alcohol"))

        df_contain_alcohol = remove_na(df_contain_alcohol, columns=['DRVR_ZIP'])

        ### Top 5 Zip codes
        result = df_contain_alcohol.groupBy(df_contain_alcohol.DRVR_ZIP).count().orderBy(F.col("count").desc()).limit(5)

        ### Writing result
        write_csv(result, file_name="result_6")

    except Exception as exp:
        logger.error("Error Occurred in the analysis_6() method. Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("analysis_6 completed, check results folder---> result_6")

