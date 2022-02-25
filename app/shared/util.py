import requests
import time
from datetime import date,datetime,timedelta
from pyspark import SparkFiles
from pyspark.sql import SparkSession
import json

def getfile(config):
    '''  Check for the file availability in Github and If the file is available there it will write it into the s3 incoming path '''
    spark=SparkSession.builder.appName(config.get("app_name")).getOrCreate()
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