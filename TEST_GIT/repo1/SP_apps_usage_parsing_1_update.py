from datetime import datetime
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_replace
import base64
import zlib
import sys
import re
import sys
import pandas as pd

spark = SparkSession.builder.getOrCreate()
date_start = str(sys.argv[1])
date_end = str(sys.argv[2])


## filter the raw

## filter the raw
def filter_the_raw(df):
    app_usg_schema = ArrayType(StringType())
    df = df.withColumn("app_usg", get_json_object(df.ds_data, '$.applicationUsageEventsInfo'))
    df= df.withColumn("app_usg", get_json_object(df.app_usg, '$.applicationUsageEventsInfoList[*]'))
    df = df.withColumn('app_usg', from_json(df.app_usg, app_usg_schema))
    df = df.withColumn('app_usg', explode(df.app_usg))
    df = df.filter(df.app_usg.isNotNull())
    df = df.withColumn('packagename', get_json_object(df.app_usg, '$.PackageName'))
    df = df.withColumn('starttime', get_json_object(df.app_usg, '$.startTime'))
    df = df.withColumn('starttime', from_unixtime(df.starttime/1000,'yyyy-MM-dd HH:mm:ss'))
    df = df.withColumn('type', get_json_object(df.app_usg, '$.type'))
    df = df.select(df.json_id, df.packagename, df.starttime, df.type, df.dt)
    df = df.filter("type in ('1', '7')")
    return df


## join the filtered parsed data with manual category system
def join_manual_category(data):
    manual_category= spark.sql("""
    select packagename
    from bdp_sz_ap_playground.apps_categories
    --where manual_category <> 'system'
    """)
    df1 = data
    final = df1.join(manual_category, on = ['packagename'], how='left')
    # df = df.filter("manual_category not like 'system'")
    return final 

## main function 
def main_script_parsing(date):
    query = f"""
        SELECT id as json_id, phone, data as ds_data, part_date  as dt
        FROM bdp_sz_ap_playground.datascore_json
        where part_date = {date}

    """
    df=spark.sql(query)
    # df.show()


    ## filter the data
    result_filter = join_manual_category(filter_the_raw(df))
    
    # ## join with the manual category
    # final_df= join_manual_category(result_filter)
    
    # ## rename the data
    return result_filter


# main program
mydates = pd.date_range(date_start, date_end).tolist()
mydates = [dates.strftime('%Y%m%d') for dates in mydates]


for date in mydates:
    print(date)
#     ## run the whole script 
#     ## get the data

    to_save= main_script_parsing(date)
    to_save = to_save.select(to_save.json_id, to_save.packagename, to_save.starttime, to_save.type, to_save.dt)
    to_save=to_save.persist()

    
    to_save.repartition(1).write.format("parquet").partitionBy("DT").mode("append").saveAsTable("bdp_sz_ap_playground.sp_apps_usage_parsed")
    to_save.unpersist()
        
    
