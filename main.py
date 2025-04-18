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

# schema = StructType([
#     StructField("id",IntegerType(),nullable=True),
#     StructField("name",StringType(),nullable=True),
#     StructField("referee_id",IntegerType(),nullable=True)

# ])

# data = [(1,"Will",None),(2,"Jane",None),(3,"Alex",2),(4,"Bill",None),(5,"Zack",1),(6,"Mark",2)]

# s3 = spark.createDataFrame(data, schema)

# s3_1 = s3.filter((col('referee_id') != 2) | (col('referee_id').isNull())).select('id','name')

# s3_1.show()


salesperson_data = [
    (1, "John", 100000, 6, "4/1/2006"),
    (2, "Amy", 12000, 5, "5/1/2010"),
    (3, "Mark", 65000, 12, "12/25/2008"),
    (4, "Pam", 25000, 25, "1/1/2005"),
    (5, "Alex", 5000, 10, "2/3/2007")
]

salesperson_schema = StructType([
    StructField("sales_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("commission_rate", IntegerType(), True),
    StructField("hire_date", StringType(), True)  # Could use DateType() if parsed properly
])

salesperson_df = spark.createDataFrame(data=salesperson_data, schema=salesperson_schema)

company_data = [
    (1, "RED", "Boston"),
    (2, "ORANGE", "New York"),
    (3, "YELLOW", "Boston"),
    (4, "GREEN", "Austin")
]

company_schema = StructType([
    StructField("com_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True)
])

company_df = spark.createDataFrame(data=company_data, schema=company_schema)


orders_data = [
    (1, "1/1/2014", 3, 4, 10000),
    (2, "2/1/2014", 4, 5, 5000),
    (3, "3/1/2014", 1, 1, 50000),
    (4, "4/1/2014", 1, 4, 25000)
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),  # Could use DateType() if parsed properly
    StructField("com_id", IntegerType(), True),
    StructField("sales_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

orders_df = spark.createDataFrame(data=orders_data, schema=orders_schema)

resu1  = orders_df.join(company_df, orders_df.com_id == company_df.com_id, how='inner') \
    .filter(orders_df.com_id == 1) \
        .select(col('sales_id'))

final_res = resu1.join(salesperson_df,resu1.sales_id == salesperson_df.sales_id,how='left_anti')

final_res.show()