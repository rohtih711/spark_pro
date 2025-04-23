from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PeopleData").getOrCreate()

data = [
    (1, 2, "2016/06/03"),
    (1, 3, "2016/06/08"),
    (2, 3, "2016/06/08"),
    (3, 4, "2016/06/09")
]


schema = StructType([
    StructField("requester_id", IntegerType(), True),
    StructField("accepter_id", IntegerType(), True),
    StructField("accept_date", StringType(), True)
])


df = spark.createDataFrame(data, schema)


col_d1 = df.selectExpr('requester_id as values')
col_d2 = df.selectExpr('accepter_id as values')

col_3 = col_d1.union(col_d2)

res1 = col_3.groupBy('values').agg(count('values').alias('count'))

win  =Window.orderBy(col('count').desc())

res2 = res1.withColumn('rank',rank().over(win))

res3 = res2.filter(col('rank') == 1).select(col('values'),col('count').alias('num'))

res3.show()

# schema = StructType([
#     StructField("person_id", IntegerType(), True),
#     StructField("person_name", StringType(), True),
#     StructField("weight", IntegerType(), True),
#     StructField("turn", IntegerType(), True)
# ])

# data = [
#     (5, "Alice", 250, 1),
#     (4, "Bob", 175, 5),
#     (3, "Alex", 350, 2),
#     (6, "John Cena", 400, 3),
#     (1, "Winston", 500, 6),
#     (2, "Marie", 200, 4)
# ]


# df = spark.createDataFrame(data, schema)


# wind  = Window.orderBy(col('turn').asc())

# df1 = df.withColumn('sum',sum('weight').over(wind))

# df2 = df1.filter(col('sum') == 1000).select('person_name')

# df2.show()