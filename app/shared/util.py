import requests
import time
from datetime import date,datetime,timedelta
from pyspark import SparkFiles
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import Window
from shared.udf import findState,extractDate,addRecovery

def getfile(spark,config):
    '''  Check for the file availability in Github and If the file is available there it will write it into the s3 incoming path '''
    url=config.get('git_url')
    dte=date.today().strftime('%m-%d-%Y')
    url=url.format(dte)
    request=requests.get(url)
    if request.status_code in range(200,300):
        spark.sparkContext.addFile(url)
        fn=str(dte)+".csv"
        bs_path=config.get('S3_Incoming')
        path=bs_path+str(dte)
        df=spark.read.csv("file://"+SparkFiles.get(fn), header=True, inferSchema= True)
        df.write.options(header=True).mode('ignore').csv(path)
#         return True
           
    else:
        print('its not downloades retrying after two minutes')
        time.sleep(12)
        getfile()

def get_date_range_daily_process():
    '''Get the daily processnig range'''
    end_dt=date.today()
    start_dt=end_dt-timedelta(14)
    dates =pd.date_range(start_dt,end_dt, freq='D')
    dates_to_download=dates.strftime('%m-%d-%Y')
    return dates_to_download

def check_availability(config):
    '''Check whether the 14 days are available in s3 Incoming or not'''
    date_available=[i[1].replace('/','') for i in dbutils.fs.ls(config.get('S3_Incoming'))]
    daily_process_dates=get_date_range_daily_process()
    for i in daily_process_dates:
        if i in date_available:
            continue
        else:
            return False
    return True

def generate_dataframe(spark,paths):
    '''Generate Dataframe and clean the data '''
    try:
        temp_df=spark.read.options(header=True,inferSchema=True).csv(paths)
        df_null=temp_df.filter(temp_df.Province_State.isNull())
        df_not_null=temp_df.filter(temp_df.Province_State.isNotNull())
        nullRemoved=df_null.withColumn('Province_State',findState(df_null.Lat,df_null.Long_))
        nullRemoved.cache()
        stateAdded_df=nullRemoved.union(df_not_null)
        dateAddedDf=stateAdded_df.withColumn('Date',extractDate(stateAdded_df.Last_Update))
        neededColumnDF=dateAddedDf.select('Province_State','Date','Confirmed','Country_Region')
        return neededColumnDF
    except:
        return 0

def genAggDF(df):
    '''To generate an aggregated datafame'''
    cdf=df.groupBy('Country_Region','Date',).agg(sum('Confirmed').alias('Total_Confirmed_Cases'))
    return cdf

def mergeWithReference(spark,df,config):
    '''Join the Dataframe with the refference DF'''
    referenceDf=spark.read.options(header=True,inferSchema=True).csv(config.get('S3_refernce'))
    joined_DF=df.join(referenceDf,'Country_Region','left').orderBy(referenceDf['per'].desc())
    filDf=joined_DF.withColumn('Recovered',addRecovery(joined_DF['Total_Confirmed_Cases'],joined_DF['per'])).groupBy('Country_region').agg(sum('Total_Confirmed_Cases').alias('total_confirmed'),sum('Recovered').alias('Total_recovered')).withColumn('Recovered_rate',addRecovrRate('total_confirmed','Total_recovered'))
    return filDf

def get_top_ten_country(df):
    '''Return a list of top ten country with decreasing rate of positive cases'''
    topTenCountry=[i[0] for i in df.orderBy(df['Total_recovered'].desc()).take(10)]
    return topTenCountry

def final_df(df,topcountry):
    '''Return a dataframe having highest positive rates in the country which we received in the args'''
    stateDF=df.filter(df['Country_Region'].isin(topcountry)).groupBy('Country_Region','Province_State').agg(sum('Confirmed').alias('Total_Confirmed'))
    rankDF=stateDF.withColumn('rank',rank().over(Window.partitionBy('Country_Region').orderBy(stateDF['Total_Confirmed'].desc())))
    mDF=rankDF.filter(rankDF['rank']<=3)
    return mDF
  
def writeouttos3(df,config):
    path=config.get('S3_output')+str(date.today().strftime('%m-%d-%Y')+'/')
    df.write.options(header=True).mode('overwrite').csv(path)

