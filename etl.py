from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import upper,split,trim,ascii,substring,substring_index
from pyspark.sql.types import DecimalType,IntegerType

spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
degree='°'

df_temps = spark.read.option("header",True).csv("../../data/global_sea_temp/SeaTemperatures.csv")
df_temps.show(10)

df_temps2 = df_temps.select(
    df_temps.City,
    substring_index(
        split(df_temps.CurrTemp,'/').getItem(0),degree,1
    ).cast(DecimalType(4,1)).alias('Temp_C'),
    substring_index(
        split(df_temps.CurrTemp,'/').getItem(1),degree,1
    ).cast(DecimalType(4,1)).alias('Temp_F'),
    substring_index(df_temps.WindSpeed,' mph',1).cast('integer').alias('WindSpeed_mph'),
    (substring_index(df_temps.Humidity,'%',1).cast('integer') / 100).alias('Humidity')
)
df_temps2.show(10)    


