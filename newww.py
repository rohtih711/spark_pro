# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
# from pyspark.sql.window import Window
# from pyspark.sql.functions import col, row_number, avg, sum, count, when

# spark = SparkSession.builder.appName("CardIssueData").getOrCreate()

# schema = StructType([
#     StructField("issue_month", IntegerType(), True),
#     StructField("issue_year", IntegerType(), True),
#     StructField("card_name", StringType(), True),
#     StructField("issued_amount", IntegerType(), True)
# ])


# data = [
#     (1, 2021, "Chase Sapphire Reserve", 170000),
#     (2, 2021, "Chase Sapphire Reserve", 175000),
#     (3, 2021, "Chase Sapphire Reserve", 180000),
#     (3, 2021, "Chase Freedom Flex", 65000),
#     (4, 2021, "Chase Freedom Flex", 70000)
# ]

# df = spark.createDataFrame(data, schema)

# wind = Window.partitionBy(col('card_name')).orderBy(col('issued_amount'))

# df1 = df.withColumn('rank', row_number().over(wind))

# res = df1.filter(col('rank') == 1).select(col('card_name'),col('issued_amount'))

# res.show()

for i in range(1000):
    print('🫂',end=" ")