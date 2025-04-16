from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import  *
from pyspark.sql.window import Window

spark = SparkSession.builder.master('local[*]').getOrCreate()


schema = StructType([
    StructField('id',IntegerType()),
    StructField("movie",StringType()),
    StructField("description",StringType()),
    StructField("rating",FloatType())
])


data = [(1,"War","great 3D",8.9), (2,"Science","fiction",8.5) ,( 3,"irish","boring",6.2) ,(4,"Ice song","Fantacy",8.6), (5,"House card","Interesting",9.1) ]


df = spark.createDataFrame(data, schema)


df1 = df.filter((col('id') %2 != 0) & (col('description')!= 'boring'))

df1.show()



schema = StructType([
    StructField("player_id",IntegerType()),
    StructField("device_id",IntegerType()),
    StructField("event_date",StringType()),
    StructField("games_played",IntegerType())
])

data = [(1,2,"2016-03-01",5),(1,2,"2016-05-02",6),(2,3,"2017-06-25",1),(3,1,"2016-03-02",0),(3,4,"2018-07-03",5)]

df = spark.createDataFrame(data, schema)

win = Window.partitionBy(col('player_id')).orderBy(col('event_date'))

df2 = df.withColumn('rank', rank().over(win))

df3 = df2.filter(col('rank') == 1).select(col('player_id'),col('event_date').alias('first_login'))

df3.show()

schema = StructType([
    StructField("id",IntegerType(),nullable=True),
    StructField("name",StringType(),nullable=True),
    StructField("referee_id",IntegerType(),nullable=True)

])

data = [(1,"Will",None),(2,"Jane",None),(3,"Alex",2),(4,"Bill",None),(5,"Zack",1),(6,"Mark",2)]

s3 = spark.createDataFrame(data, schema)

s3_1 = s3.filter((col('referee_id') != 2) | (col('referee_id').isNull())).select('id','name')

s3_1.show()