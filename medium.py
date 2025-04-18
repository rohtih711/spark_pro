from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PeopleData").getOrCreate()


schema = StructType([
    StructField("person_id", IntegerType(), True),
    StructField("person_name", StringType(), True),
    StructField("weight", IntegerType(), True),
    StructField("turn", IntegerType(), True)
])

data = [
    (5, "Alice", 250, 1),
    (4, "Bob", 175, 5),
    (3, "Alex", 350, 2),
    (6, "John Cena", 400, 3),
    (1, "Winston", 500, 6),
    (2, "Marie", 200, 4)
]


df = spark.createDataFrame(data, schema)


wind  = Window.orderBy(col('turn').asc())

df1 = df.withColumn('sum',sum('weight').over(wind))

df2 = df1.filter(col('sum') == 1000).select('person_name')

df2.show()