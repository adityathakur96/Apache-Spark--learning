# trying to do myself and little bit with chat gpt 
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel+Names")

lines = spark.read.text("file:///SparkCourse/Marvel+Graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# connectionsWithOneEntry = connections.filter(func.col("connections") == 1).alias("onlyOne") \
#     .join(func.col("id"),names.filter(func.col("connections") == 1)).select("name")

# Filter superheroes with only one connection
connectionsWithOne = connections.filter(func.col("connections") == 1)
# learning dont hard code the two give only specific like one , zero do for all the cases like in actual solution of frank where 
# it gave the lowest of all the connection that should be zero 
\
# Join connections with names to get superhero names
connectionsWithOneEntry = connectionsWithOne.join(names, "id") # join aise hi lagta hai uper jaise comment kar rakha hai vaise nahi 

for row in connectionsWithOneEntry.collect():
    print(f"{row['name']} has only {row['connections']} connection.")




# mostObscure = connections.sort(func.col("connections"))
# mostObscureName = names.filter(func.col("id") == mostObscure[0]).select("name").first()





# below is the one of extra credit that compute the actuall smallest number connections in the dataset instead of assuming it is one




# from pyspark.sql import SparkSession
# from pyspark.sql import functions as func
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

# schema = StructType([ \
#                      StructField("id", IntegerType(), True), \
#                      StructField("name", StringType(), True)])

# names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel+Names")

# lines = spark.read.text("file:///SparkCourse/Marvel+Graph")

# # Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# # throw off the counts.
# connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
#     .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
#     .groupBy("id").agg(func.sum("connections").alias("connections"))


# mostObscure = connections.sort(func.col("connections")).first()
# mostObscureName = names.filter(func.col("id") == mostObscure[0]).select("name").first()

# print(mostObscureName[0] + " is the most obscure superhero with " + str(mostObscure[1]) + " co-appearances.")

# mostPopular = connections.sort(func.col("connections").desc()).first()


# mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()


# print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")



