from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import upper,split,trim,ascii,substring,substring_index
from pyspark.sql.types import DecimalType,IntegerType
import sys
import boto3

spark = SparkSession.builder.appName("Basit ETL").getOrCreate()
degree='°'

if len(sys.argv) != 4:
    print('Usage: etl.py temps cities countries')
    exit(1)

f_temps = sys.argv[1]
f_cities = sys.argv[2]
f_countries = sys.argv[3]


df_temps = spark.read \
        .option("header","true") \
        .csv(f_temps)

df_cities = spark.read \
        .option("header","true") \
        .csv(f_cities)

df_countries = spark.read \
        .option("header","true") \
        .csv(f_countries)

df_countries = df_countries.select(
    trim(df_countries.Continent).alias('Continent'),
    trim(df_countries.Country).alias('Country')
)

df_temps_clean = df_temps.select(
        df_temps.City,
        substring_index(split(df_temps.CurrTemp,'/').getItem(0),degree,1)\
                .cast(DecimalType(4,1))
                .alias('temp_c'),
        substring_index(split(df_temps.CurrTemp,'/').getItem(1),degree,1)\
                .cast(DecimalType(4,1))\
                .alias('temp_f'),
        substring_index(df_temps.WindSpeed,' mph',1)\
                .cast('integer')\
                .alias('windspeed_mph'),
        substring_index(df_temps.Humidity,'%',1)\
                .cast('integer')\
                .alias('humidity_pct')
                )


df_temps2 = df_temps_clean\
    .join(df_cities, ['City'])\
    .join(df_countries,['Country'])

df_temps2.show(10) 
