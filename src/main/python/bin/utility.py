import pyspark.sql.functions as F
from pyspark.sql.window import Window
import get_all_variables as var
import pandas

def replace_null_with_char( df, columns, char):
    for column in columns:
        df = df.withColumn(column, F.coalesce(F.col(column), F.lit(char)))
    return df

def remove_na( df, columns):
    for column in columns:
        df = df.filter( F.col(column) != "na")
    return df

def remove_null(df, columns):
    for column in columns:
        df = df.filter(column + " IS NOT NULL ")
    return df

def lower_convert( df, columns):
    for column in columns:
        df = df.withColumn(column, F.lower(column))
    return df

def remove_trim( df, columns):
    for column in columns:
        df = df.withColumn(column, F.trim(column))
    return df

def file_write(content, file_name):
    f = open(var.output + file_name +".txt", "w")
    f.write(content)
    f.close()

def write_csv(df, file_name):
    pd_df = df.toPandas()
    pd_df.to_csv(var.output + file_name +".csv", index=False)

