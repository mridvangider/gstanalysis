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


    df_out = df_temps_clean\
        .join(df_cities, ['City'],'leftouter')\
        .join(df_countries,['Country'],'leftouter')\
        .select('Continent','Country','City','Temp_C','Temp_F','WindSpeed_Mph','Humidity_pct')

    df_out.write.option('header','true').csv(output)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--temps', help="Temperatures file")
    parser.add_argument('--cities', help="Cities file")
    parser.add_argument('--countries', help="Countries file")
    parser.add_argument('--output', help="Output file")

    args = parser.parse_args()
    process(args.temps,args.cities,args.countries,args.output)

