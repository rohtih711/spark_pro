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

#customerdf.show()

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

active_customers_tagged_df = final_df.where(col("active_flag") == True) 

inactive_customers_tagged_df = final_df.where(col("active_flag") == False)

join_df = active_customers_tagged_df.join(customerdf,'customerID','full_outer')

def column_rename(df,suffix,append):
    if append:
        new_column_names = list(map(lambda x: x+ suffix, df.columns))
    else:   
        new_column_names = list(map(lambda x: x.replace(suffix,""), df.columns))
    
    return df.toDF(*new_column_names)

def get_hash(df,key_list):
    columns = [col(column) for column in key_list]

    if columns:
        return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
    else:
        return df.withColumn("hash_md5", md5(lit(1)))

active_customers_tagged_df_hash = column_rename(get_hash(active_customers_tagged_df,slowly_changing_cols),suffix="_target",append=True)

customerdf_hash = column_rename(get_hash(customerdf,slowly_changing_cols),suffix="_source",append=True)

merged_df = active_customers_tagged_df_hash.join(customerdf_hash,col('customerID_source') == col('customerID_target'),'full_outer') \
.withColumn("Action", when(col("hash_md5_source") == col("hash_md5_target"), "NoChange") \
            .when(col("customerID_source").isNull(), 'DETETE') \
            .when(col("customerID_target").isNull(), 'INSERT') \
            .otherwise('UPDATE')) \
            
merged_df.show()