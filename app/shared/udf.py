from datetime import datetime,date,timedelta
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from geopy.geocoders import Nominatim

def calc_recovry_rate(total_con,Recovered):
    '''Calculate the recovery rate based on the reffernce data'''
    recove_rate=Recovered*100
    return recove_rate/total_con

def calc_recovery(total_con,perc):
    recov=(total_con*perc)/100
    return int(recov)


def add_date(x):
    ''' Extract date from datetime '''
    extracted_date=datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date()
    return str(extracted_date)

def find_state(lat,longi):
    '''find the state based on the longitude and latitude'''
    geolocator=Nominatim(user_agent="geoapiExercises")
    try:
        location = geolocator.reverse(str(lat)+","+str(longi))
        return location.raw['address']['state']
    except:
        return "Unknown"
    
findState=udf(lambda x,y:find_state(x,y),StringType())
extractDate=udf(lambda x:add_date(x),StringType())
addRecovery=udf(lambda x,y:calc_recovery(x,y))
addRecovrRate=udf(lambda x,y:calc_recovry_rate(x,y))