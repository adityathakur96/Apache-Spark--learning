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


# Every line represents a given superhero ID and a list of all their connections. We take that raw line of input.
# We extract off the first number by splitting everything by spaces and pulling off that first entry, 
# and that's the hero ID that we're talking about. And then we, again, split the entire row by spaces and just count up how many
# individual entries we get as a result. We subtract one to subtract off that first element,
# and that gives us the count of how many connections are associated with each hero ID for that line.
mostPopular = connections.sort(func.col("connections").desc()).first()

# So we got back most popular which is a DataFrame containing just one row that contains the most popular superhero.
# which has one row that is supperhero id and connections of it 

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()
# So zero refers to the first column of that result, which should contain the superhero ID.
# So ideally there should be only one entry for a given superhero ID, but if by some chance there's more than one,
# we'll just take the first entry for its name.

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")



# output : CAPTAIN AMERICA is the most popular superhero with 1933 co-appearances.