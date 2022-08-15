from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import upper,split,trim,ascii,substring,substring_index
from pyspark.sql.types import DecimalType,IntegerType
import sys
import boto3

spark = SparkSession.builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.17").getOrCreate()
degree='°'

if len(sys.argv) != 3:
    print('Usage: etl.py <DBUSER> <DBPASSWORD>')
    exit(1)

dbuser = sys.argv[1]
dbpass = sys.argv[2]

jdbc_url = 'jdbc:mysql://dbinstance1.cvx8acmkmdhd.eu-central-1.rds.amazonaws.com/dbase1'

df_temps = spark.read.format("jdbc") \
    .option("url",jdbc_url)\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("dbtable","sea_temps")\
    .option("user",dbuser)\
    .option("password",dbpass)\
    .load()

df_cities = spark.read.format("jdbc") \
    .option("url",jdbc_url)\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("dbtable","cities")\
    .option("user",dbuser)\
    .option("password",dbpass)\
    .load()

df_countries = spark.read.format("jdbc") \
    .option("url",jdbc_url)\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("dbtable","countries")\
    .option("user",dbuser)\
    .option("password",dbpass)\
    .load()

df_temps2 = df_temps\
    .join(df_cities, 'city')\
    .join(df_countries,'country')\
    .select(
        df_countries.continent,
        df_cities.country,
        df_temps.city,
        substring_index(
            split(df_temps.currtemp,'/').getItem(0),degree,1
        ).cast(DecimalType(4,1)).alias('Temp_C'),
        substring_index(
            split(df_temps.currtemp,'/').getItem(1),degree,1
        ).cast(DecimalType(4,1)).alias('Temp_F'),
        substring_index(df_temps.windspeed,' mph',1).cast('integer').alias('WindSpeed_mph'),
        (substring_index(df_temps.humidity,'%',1).cast('integer') / 100).alias('Humidity')
    )

df_temps2.show(10)    