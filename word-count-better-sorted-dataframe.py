from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("file:///SparkCourse/book.txt")  # its different from other code in data as that code for structured data and this is for un structured data here

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count()) # this will show every element  not limiting to 20 
