from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').getOrCreate()

flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),
                (1,'Flight2' , 'Hyderabad' , 'Kochi'),
                (1,'Flight3' , 'Kochi' , 'Mangalore'),
                (2,'Flight1' , 'Mumbai' , 'Ayodhya'),
                (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')
                ]

_schema = "cust_id int, flight_id string , origin string , destination string"

df_flight = spark.createDataFrame(data = flights_data , schema= _schema)

wind = Window.partitionBy(col('cust_id')).orderBy(col('flight_id').asc())

df_flight1 = df_flight.withColumn('rs',rank().over(wind))

df_flight2 = df_flight1.groupBy(col('cust_id')).agg(min(col('rs')).alias('min'),max(col('rs')).alias('max'))

#df_flight2.show()

join_df = df_flight1.join(df_flight2, df_flight1.cust_id == df_flight2.cust_id,'inner').drop(df_flight2.cust_id)


final_df = join_df.groupBy(col('cust_id')).agg(
    max(when(col('rs') == col('min'),col('origin'))).alias('origin'),
    min(when(col('rs') == col('max'),col('destination'))).alias('destination')
)

final_df.show()
