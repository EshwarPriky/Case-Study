
import os
import get_all_variables as var
from create_spark import create_spark_object
from validation import get_spark_id
from file_ingestion import load_file
import sys
import logging
import logging.config
from analysis_1 import analysis_1
from analysis_2 import analysis_2
from analysis_3 import analysis_3
from analysis_4 import analysis_4
from analysis_5 import analysis_5
from analysis_6 import analysis_6
from analysis_7 import analysis_7
from analysis_8 import analysis_8



### Load the Logging Configuraion file
logging.config.fileConfig(fname='../config/logging_to_file.conf')


def main():
    # get variables
    # create spark object
    # initiate data injection
    # run analysis and write
    try:
        logging.info("main() is started...")

        ### Create Spark Object
        spark = create_spark_object( var.envn, var.appName)
        ### Validate created Spark object
        get_spark_id(spark)


        ### Load File as Dataframe
        logging.info("Ingestion is started...")
        df_charges = load_file(spark, var.file_name[0], var.file_path[0],var.header, var.inferSchema )
        df_damages = load_file(spark, var.file_name[1], var.file_path[1], var.header, var.inferSchema)
        df_endorse = load_file(spark, var.file_name[2], var.file_path[2], var.header, var.inferSchema)
        df_primary_person = load_file(spark, var.file_name[3], var.file_path[3], var.header, var.inferSchema)
        df_restrict = load_file(spark, var.file_name[4], var.file_path[4], var.header, var.inferSchema)
        df_units = load_file(spark, var.file_name[5], var.file_path[5], var.header, var.inferSchema)

        logging.info("Analysis started...")
        ### Analysis 1
        analysis_1(df_primary_person)

        ### Analysis 1
        analysis_2(df_units)

        ### Analysys 3
        analysis_3(df_primary_person)

        ### Analysis 4
        analysis_4(df_units)

        ### Analysis 5
        analysis_5(df_primary_person, df_units)

        ### Analysis 6
        analysis_6(df_primary_person, df_units)

        ### Analysis 7
        analysis_7( df_units,df_damages )

        ### Analysis 8
        analysis_8(df_charges, df_primary_person, df_units, df_restrict)

        logging.info("Analysis Completed...")
        logging.info("Pipeline is completed...")


    except Exception as exp:
        logging.error("Error Occurred in the main() method. Please check the Stack Trace to go to the respective module and fix it. " + str(exp), exc_info = True)
        sys.exit(1)

    pass


if __name__ == "__main__":
    logging.info("Pipeline is started...")
    main()


