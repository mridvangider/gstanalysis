import argparse

from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import upper,split,trim,ascii,substring,substring_index
from pyspark.sql.types import DecimalType,IntegerType
import sys

def process(temps, cities, countries, output):
    if debug:
        print('gst-etl: Inside the process()')
        print('gst-etl: Creating the SparkSession')

    spark = SparkSession.builder.appName("Basit ETL").getOrCreate()
    degree='°'

    if debug:
        print('gst-etl: Reading temps')

    df_temps = spark.read \
            .option("header","true") \
            .csv(temps)
    if debug:
        print('gst-etl: Reading cities')

    df_cities = spark.read \
            .option("header","true") \
            .csv(cities)
    if debug:
        print('gst-etl: Reading countries')

    df_countries = spark.read \
            .option("header","true") \
            .csv(countries)
    if debug:
        print('gst-etl: Cleaning df_countries')

    df_countries = df_countries.select(
        trim(df_countries.Continent).alias('Continent'),
        trim(df_countries.Country).alias('Country')
    )

    if debug:
        print('gst-etl: Cleaning df_temps')

    df_temps_clean = df_temps.select(
            df_temps.City,
            substring_index(split(df_temps.CurrTemp,'/').getItem(0),degree,1)\
                    .cast(DecimalType(4,1))
                    .alias('Temp_C'),
            substring_index(split(df_temps.CurrTemp,'/').getItem(1),degree,1)\
                    .cast(DecimalType(4,1))\
                    .alias('Temp_F'),
            substring_index(df_temps.WindSpeed,' mph',1)\
                    .cast('integer')\
                    .alias('WindSpeed_Mph'),
            substring_index(df_temps.Humidity,'%',1)\
                    .cast('integer')\
                    .alias('Humidity_pct')
                    )

    if debug:
        print('gst-etl: Createing df_out')

    df_out = df_temps_clean\
        .join(df_cities, ['City'],'leftouter')\
        .join(df_countries,['Country'],'leftouter')\
        .select('Continent','Country','City','Temp_C','Temp_F','WindSpeed_Mph','Humidity_pct')

    if debug:
        print('gst-etl: Writing df_out')

    df_out.write.option('header','true').partitionBy('Continent').mode('overwrite').csv(output)

    if debug:
        print('gst-etl: Exitting process()')

debug = False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--temps', help="Temperatures file")
    parser.add_argument('--cities', help="Cities file")
    parser.add_argument('--countries', help="Countries file")
    parser.add_argument('--output', help="Output file")
    parser.add_argument('--debug', help="Enable debug messages", action='store_true')

    args = parser.parse_args()
    
    if args.debug:
        debug = True

    if debug:
        print('gst-etl: Parsed the arguments')
        print('gst-etl: Calling process()')
    process(args.temps,args.cities,args.countries,args.output)

    print('gst-etl: Exitting __main__')

