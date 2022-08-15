from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import upper,split,trim,ascii,substring,substring_index
from pyspark.sql.types import DecimalType,IntegerType
import sys


spark = SparkSession.builder.getOrCreate()
# spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
degree='°'

if len(sys.argv) != 3:
    print('Usage: etl.py <DBUSER> <DBPASSWORD>')
    exit(1)

dbuser = sys.argv[1]
dbpass = sys.argv[2]

# df_temps = spark.read.option("header",True).csv("../../data/global_sea_temp/SeaTemperatures.csv")
# df_temps.show(10)

jdbc_url = 'jdbc:mysql://dbinstance1.cvx8acmkmdhd.eu-central-1.rds.amazonaws.com/dbase1'

df_temps = spark.read.jsbc(jdbc_url,'sea_temps',properties={'user':dbuser,'password':dbpass})
# df_cities = spark.read.jdbc(jdbc_url,'cities')
# df_countries = spark.read.jdbc(jdbc_url,'countries')

# df_temps2 = df_temps.select(
#     df_temps.City,
#     substring_index(
#         split(df_temps.CurrTemp,'/').getItem(0),degree,1
#     ).cast(DecimalType(4,1)).alias('Temp_C'),
#     substring_index(
#         split(df_temps.CurrTemp,'/').getItem(1),degree,1
#     ).cast(DecimalType(4,1)).alias('Temp_F'),
#     substring_index(df_temps.WindSpeed,' mph',1).cast('integer').alias('WindSpeed_mph'),
#     (substring_index(df_temps.Humidity,'%',1).cast('integer') / 100).alias('Humidity')
# )
# df_temps2.show(10)    

df_temps.show(10)
