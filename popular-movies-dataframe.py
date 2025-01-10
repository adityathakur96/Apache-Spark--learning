from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema when reading u.data
schema1 = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema1).csv("file:///SparkCourse/ml-100k/u.data") 
# here seperator is not comma seperated it is tab seperated value that why using read.option sep as tab t 

# Some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()
