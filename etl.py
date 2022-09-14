import argparse

from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import upper,split,trim,ascii,substring,substring_index
from pyspark.sql.types import DecimalType,IntegerType
import sys
import boto3

def process(temps, cities, countries, output):
    spark = SparkSession.builder.appName("Basit ETL").getOrCreate()
    degree='°'

    df_temps = spark.read \
            .option("header","true") \
            .csv(temps)

    df_cities = spark.read \
            .option("header","true") \
            .csv(cities)

    df_countries = spark.read \
            .option("header","true") \
            .csv(countries)

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


    df_out = df_temps_clean\
        .join(df_cities, ['City'])\
        .join(df_countries,['Country'])

    df_out.write.csv(output)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--temps', help="Temperatures file")
    parser.add_argument('--cities', help="Cities file")
    parser.add_argument('--countries', help="Countries file")
    parser.add_argument('--output', help="Output file")

    process(temps,cities,countries,output)

