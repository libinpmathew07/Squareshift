from tkinter.tix import Tree
from pyspark.sql import SparkSession
import json
with open("config.json","r") as config_file:
    config=json.load(config_file)

spark=SparkSession.builder.appName(config.get("app_name")).getOrCreate()
df=spark.read.options(header=True).csv(config.get("Source_path"))
df.show()