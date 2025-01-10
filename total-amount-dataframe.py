# mene khud toh kiya but thpoda frank ka bhi dekha sorting or thoda last me error arraha tha 

from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalAmountSpent").getOrCreate()



schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("orderID", IntegerType(), True), \
                     StructField("Amount_Spent", FloatType(), True)])

# Read each line of my book into a dataframe
df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv") 
df.printSchema()

selectedTerms = df.select("customerID", "Amount_Spent")


TotalAmountByCustomer = selectedTerms.groupBy("customerID").agg(func.round(func.sum("Amount_Spent"), 2).alias("Total_amount_Spent"))
# alias always comes under agg(aggregation)

sortedByCustomer = TotalAmountByCustomer.sort("Total_amount_Spent")

sortedByCustomer.show(sortedByCustomer.count())

spark.stop()    




