from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# JSON-like data (Python list of dicts)
data = [
    {
        "id": 1,
        "name": "Alice",
        "department": "HR",
        "address": {
            "city": "New York",
            "state": "NY"
        }
    },
    {
        "id": 2,
        "name": "Bob",
        "department": "IT",
        "address": {
            "city": "San Francisco",
            "state": "CA"
        }
    },
    {
        "id": 3,
        "name": "Charlie",
        "department": "Finance",
        "address": {
            "city": "Chicago",
            "state": "IL"
        }
    }
]

df = spark.createDataFrame(data)


df.printSchema()
res1 = df.select('id','name','department',col('address.city').alias('city'),col('address.state').alias('state'))

re2 = res1.repartition(1)


try:
    re2.write.format('parquet').mode('overwrite').save('newdata.parquet')
    print("done")
except Exception as e:
    print(e)