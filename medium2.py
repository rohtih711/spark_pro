from pyspark.sql import SparkSession
from pyspark.sql.functions import *    
from pyspark.sql.window import Window


spark = SparkSession.builder.master('local[*]').appName('example').getOrCreate()



data = [ ("East", "Jan", 200), ("East", "Feb", 300), 
("East", "Mar", 250), ("West", "Jan", 400), 
("West", "Feb", 350), ("West", "Mar", 450) ]



columns = ["Region", "Month", "Sales"]


df =spark.createDataFrame(data, columns)

wind  = Window.partitionBy(col('Region')).orderBy(col('Sales'))

df1 = df.withColumn('sums',sum(col('Sales')).over(wind)).withColumn('rank',rank().over(wind))

df1.show()
