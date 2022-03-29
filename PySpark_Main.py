from time import time,sleep
import nsepy
import datetime
import pandas
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import substring
from pyspark.sql.types import StringType,StructType,StructField,FloatType
import pandas as pd


#This whole content will be executed every 1 Minute
#Extracting data from NSE using nsepy
while True:
    sleep(60 - time() % 60)
    def get_data(symbol):
        live_data = {}
        '''
        Open = float(nsepy.get_quote(symbol)["data"][0]["open"].replace(',',''))
        close = float(nsepy.get_quote(symbol)["data"][0]["closePrice"].replace(',',''))
        lastPrice = float(nsepy.get_quote(symbol)["data"][0]["lastPrice"].replace(',',''))
        #lastUpdatedTime = float(nsepy.get_quote(symbol)["data"][0]["lastUpdateTime"])
        dayLow = float(nsepy.get_quote(symbol)["data"][0]["dayLow"].replace(',',''))
        dayHigh = float(nsepy.get_quote(symbol)["data"][0]["dayHigh"].replace(',',''))
        basePrice = float(nsepy.get_quote(symbol)["data"][0]["basePrice"].replace(',',''))
        Company = str(nsepy.get_quote(symbol)["data"][0]["symbol"])
        Time = str(datetime.datetime.now()).split('.')[0]
        print(Time)'''

        live_data = {"open":float(nsepy.get_quote(symbol)["data"][0]["open"].replace(',','')),
                             "close":float(nsepy.get_quote(symbol)["data"][0]["closePrice"].replace(',', '')),
                             "lastPrice":float(nsepy.get_quote(symbol)["data"][0]["lastPrice"].replace(',', '')),
                             "dayLow":float(nsepy.get_quote(symbol)["data"][0]["dayLow"].replace(',', '')),
                             "dayHigh":float(nsepy.get_quote(symbol)["data"][0]["dayHigh"].replace(',', '')),
                             "basePrice":float(nsepy.get_quote(symbol)["data"][0]["basePrice"].replace(',', '')),
                             "Time":str(datetime.datetime.now()).split('.')[0],
                             "Company":str(nsepy.get_quote(symbol)["data"][0]["symbol"]),
                             "previousClose": float(nsepy.get_quote(symbol)["data"][0]["previousClose"].replace(',', ''))
                             }
        return(live_data)

    #Creating spark Session
    symbol = "TCS"
    spark = SparkSession.builder.appName("nse_live") \
                        .config("spark.sql.streaming.schemaInference", "true") \
                        .getOrCreate()

    #Creating a DataFrame
    data = []
    data = [get_data(symbol)]
    rows = [Row(**i) for i in data]
    df = spark.createDataFrame(rows)

    #extract date from timestamp
    pd_date_df = df.select('*',substring('Time',0,10).alias('Date'))
    today_date = datetime.date.today()
    pandas_df = pd_date_df.toPandas()

    #Run this once(first_time) and comment it - this is to print header
    #pandas_df.to_csv('nse_live_data.csv', mode='a', index=False, header=True)

    #Uncomment below line from second time,contains all day data from which this program starts to executed
    pandas_df.to_csv('nse_live_data.csv', mode='a', index=False, header=False)

    #Schema definition
    data_schema = StructType([  StructField('open',FloatType()),
                                StructField('close',FloatType()),
                                StructField('lastPrice',FloatType()),
                                StructField('dayLow',FloatType()),
                                StructField('dayHigh',FloatType()),
                                StructField('basePrice',FloatType()),
                                StructField('Time',StringType()),
                                StructField('Company',StringType()),
                                StructField('previousClose',FloatType()),
                                StructField('Date',StringType())
                             ])
    spark_df = spark.read.csv('nse_live_data.csv',schema=data_schema,header=False)
    #spark_df.printSchema()

    #Transformation to get only Today's data from everyday's data
    s_date_df = spark_df.drop('Date') \
                .select('*',substring('Time',0,10).alias('Date'),substring('Time',11,6).alias('Time1')).drop('Time')
    today_date = datetime.date.today()
    daily_df = s_date_df.filter(s_date_df['Date'] == today_date)
    daily_df.show()
    pd_daily_df = daily_df.toPandas()
    #Sinking daily data into CSV File
    pd_daily_df.to_csv('nse_live_graph.csv', mode='w', index=False, header=True)

#static Graph-working
'''x=pd_daily_df['Time1'].to_list()
y=pd_daily_df['lastPrice'].to_list()
plt.plot(x,y)
plt.scatter(x,y)
plt.title("TCS NSE live stock data")
plt.xlabel("Time")
plt.ylabel("lastPrice")
plt.xticks(rotation=90)
#plt.ylim(bottom=pd_,top=(top_value+500))
plt.legend()
plt.show()'''
