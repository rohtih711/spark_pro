from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import  *


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