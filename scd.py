from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SCDExample").getOrCreate()

DATE_FORMAT = "yyyy-MM-dd"
future_data = "9999-12-31"
primary_key = ["customerid"]
slowly_changing_cols = ["email", "phone", "address","city", "state", "zipcode"]
implementation_cols = ["effective_date", "end_date", "active_flag"]

costomers_source_schema = "customerID long, FirstName string, LastName string, Email string, Phone string, Address string, City string, State string, ZipCode string"

customer_target_schema = "customerID long, FirstName string, LastName string, Email string, Phone string, Address string, City string, State string, ZipCode long, customer_skey long,effective_date date, end_date date, active_flag boolean"

customerdf = spark.read.format("csv").option("header", "true").schema(costomers_source_schema).load("/Users/rohithb/Desktop/new_psark/orders.csv")

winddow = Window.orderBy("customerID")

enhanced_customer_df = customerdf \
                  .withColumn("customer_skey", row_number().over(winddow)) \
                  .withColumn("effective_date", date_format(current_date(),DATE_FORMAT)) \
                  .withColumn("end_date", date_format(lit(future_data), DATE_FORMAT)) \
                  .withColumn("active_flag", lit(True)) 


# try:
#     enhanced_customer_df.write.format('csv').mode('overwrite').save('/Users/rohithb/Desktop/new_psark/destionation.csv')
#     print("Data written successfully.")
# except Exception as e:
#     print(f"Error writing data: {e}")


final_df = spark.read.format("csv").option("header", "true").schema(customer_target_schema).load("/Users/rohithb/Desktop/new_psark/destionation.csv")

max_sk = final_df.agg({"customer_skey": "max"}).collect()[0][0] 

print(max_sk)
