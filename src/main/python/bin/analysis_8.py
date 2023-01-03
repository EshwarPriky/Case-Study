from utility import *
import logging
import logging.config

logging.config.fileConfig(fname = '../config/logging_to_file.conf')
logger = logging.getLogger("analysis")


def analysis_8(df, df2, df3, df4):

    '''

    :param df: Charges data
    :param df2: Primary_person data
    :param df3: Units data
    :param df4: Restrict data
    :return: result_8 file in results directory

    Top 5 Vehicle Makes where drivers are charged with speeding related offences,
    has licensed Drivers,
    used top 10 used vehicle colours,
    and has car licensed with the Top 25 states with highest number of offences

    '''

    try:
        ###Speeding Related offence
        df = replace_null_with_char(df, columns=['CHARGE'], char='NA')
        df = lower_convert(df, columns=['CHARGE'])
        df_speed_charge = df.filter(df.CHARGE.like("%speed%"))
        df_speed_charge = df_speed_charge.groupBy("CRASH_ID", 'UNIT_NBR').count()

        ### Driver's Licensed
        df2 = replace_null_with_char(df2, columns=['DRVR_LIC_TYPE_ID'], char='NA')
        df2 = lower_convert(df2, columns=['DRVR_LIC_TYPE_ID'])
        df2 = remove_trim(df2, columns=['DRVR_LIC_TYPE_ID'])
        df_licensed_driver = df2.filter(df2.DRVR_LIC_TYPE_ID.isin(["driver license", "commercial driver lic."]))

        ### Top 10 used colour
        df3 = replace_null_with_char(df3, columns=['VEH_COLOR_ID'], char="NA")
        df3 = lower_convert(df3, columns=['VEH_COLOR_ID'])
        df3 = remove_trim(df3, columns=['VEH_COLOR_ID'])
        df3 = remove_na(df3, columns=['VEH_COLOR_ID'])
        top_colour = df3.groupBy("VEH_COLOR_ID").count().orderBy(F.col("count").desc()).take(10)
        colour = [i for i, j in top_colour]
        df_top_color = df3.filter(df3.VEH_COLOR_ID.isin(colour))

        ### Top 25 states with most offence
        ### Offended person has restricted driver license

        df3 = replace_null_with_char(df3, columns=['VEH_LIC_STATE_ID'], char="NA")
        df3 = lower_convert(df3, columns=['VEH_LIC_STATE_ID'])
        df3 = remove_trim(df3, columns=['VEH_LIC_STATE_ID'])
        df3 = remove_na(df3, columns=['VEH_LIC_STATE_ID'])

        cond = [(df4.CRASH_ID == df3.CRASH_ID) & (df4.UNIT_NBR == df3.UNIT_NBR)]
        joined_df = df4.join(df3, cond, 'inner').select(df3.CRASH_ID, df3.UNIT_NBR, df3.VEH_LIC_STATE_ID)

        joined_df = joined_df.groupBy("VEH_LIC_STATE_ID").count().orderBy(F.col('count').desc()).take(25)
        top_25_states = [i for i, j in joined_df]
        df_top_25_states = df3.filter(df3.VEH_LIC_STATE_ID.isin(top_25_states))

        ### Join all the results
        cond1 = [(df_speed_charge.CRASH_ID == df_top_color.CRASH_ID)
                 & (df_speed_charge.UNIT_NBR == df_top_color.UNIT_NBR)]

        join_1 = (df_speed_charge.join(df_top_color, cond1, 'inner')
                  .select(df_speed_charge.CRASH_ID, df_speed_charge.UNIT_NBR))

        cond2 = [(join_1.CRASH_ID == df_top_25_states.CRASH_ID)
                 & (join_1.UNIT_NBR == df_top_25_states.UNIT_NBR)]

        join_2 = (join_1.join(df_top_25_states, cond2, 'inner')
                  .select(join_1.CRASH_ID, join_1.UNIT_NBR))

        cond3 = [(join_2.CRASH_ID == df_licensed_driver.CRASH_ID)
                 & (join_2.UNIT_NBR == df_licensed_driver.UNIT_NBR)]

        join_3 = (join_2.join(df_licensed_driver, cond3, 'inner')
                  .select(join_2.CRASH_ID, join_2.UNIT_NBR))

        cond4 = [(join_3.CRASH_ID == df3.CRASH_ID)
                 & (join_3.UNIT_NBR == df3.UNIT_NBR)]

        top_car_makes = join_3.join(df3.alias('df3'), cond4, 'inner').select(F.col('df3.*'))

        ### Top Car makers
        top_car_makes = top_car_makes.groupBy("VEH_MAKE_ID").count().orderBy(F.col("count").desc()).limit(5)

        ### Writing result
        write_csv(top_car_makes, file_name="result_8")

    except Exception as exp:
        logger.error("Error Occurred in the analysis_8() method. Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("analysis_8 completed, check results folder---> result_8")

