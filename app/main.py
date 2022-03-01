from pyspark.sql import SparkSession
from jobs.dailyjob import jobrun
import json

with open("config.json","r") as config_file:
    config=json.load(config_file)

spark=SparkSession.builder.appName(config.get("app_name")).getOrCreate()

if __name__=='__main__':
    jobrun(spark,config)
