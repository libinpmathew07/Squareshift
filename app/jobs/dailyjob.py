from shared.udf import findState,addRecovery,addRecovrRate,extractDate
from shared.util import genAggDF,getfile,get_top_ten_country,generate_dataframe,get_date_range_daily_process,writeouttos3,mergeWithReference,final_df,check_availability

def extract_data(spark,config):
    m=getfile(spark,config)

def transformData(spark,config):
    bp=config.get('s3_incoming')
    paths=[bp+i+'/' for i in get_date_range_daily_process()]
    availableInIncoming=check_availability()
    if availableInIncoming:
        df1=generate_dataframe(spark,paths) 
        df=genAggDF(df1)
        joinedDF=mergeWithReference(df)
        topcountry=get_top_ten_country(joinedDF)
        stDF=final_df(df1,topcountry)
        return stDF
    else:
        print("Required dates are not available in S3 Incoming")

def loadDataFrame(config,stDF):
    writeouttos3(config,stDF)


def jobrun(spark,config):
    extract_data(spark,config)
    rawdf=transformData(spark,config)
    loadDataFrame(config,rawdf)
    


