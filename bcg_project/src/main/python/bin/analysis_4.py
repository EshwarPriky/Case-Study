from utility import *
from pyspark.sql.window import Window
import logging
import logging.config


logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("analysis")


def analysis_4(df):
    '''

        :param df: Units data
        :return: result_4 file in results directory

        Top 5 to 15 Make_Ids contributing to injury or death

    '''

    try:
        ### ### Replace null with 0
        df_null_replace = replace_null_with_char(df, columns=["TOT_INJRY_CNT", "DEATH_CNT"], char='0')


        ### Converting columns to lower case
        df_lower = lower_convert(df_null_replace, columns=['VEH_MAKE_ID'])

        ### 'Total' column and filtering 'NA'
        df_total_count = (
            df_lower.withColumn("total_count", F.col("TOT_INJRY_CNT").cast('int') + F.col("DEATH_CNT").cast('int'))
            .filter("VEH_MAKE_ID <> 'na'")
        )

        ### Grouping by Make Ids
        df_grp = (df_total_count.groupBy("VEH_MAKE_ID").sum("total_count").withColumnRenamed("sum(total_count)", "total")
                  .orderBy(F.col("total").desc()))

        ### Creating dummy column for psedo-partition
        df_grp = df_grp.withColumn("dummy", F.lit("dummy"))
        win = Window.partitionBy("dummy").orderBy(F.col("total").desc())

        ### Top 5 to 15 Make ids
        df_grp = df_grp.withColumn("Row_Number", F.row_number().over(win))
        maker_id = df_grp.filter(F.col('Row_Number').between(5, 15)).select("Row_Number","VEH_MAKE_ID" )

        ### Writing result
        write_csv(maker_id, file_name="result_4")

    except Exception as exp:
        logger.error("Error Occurred in the analysis_4() method. Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("analysis_4 completed, check results folder---> result_4")
