from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")


print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("age", "friends").show()

print("Group by age")
people.groupBy("age").avg("friends").orderBy("age").show()