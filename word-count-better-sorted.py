import re # here we have used the regular expresion 
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y) 
# idhar map har word ke samne 1 dalra hai toh mtlb ye ek key\value ban gya hai baad me reduceByKey same keys ki values ko add kardega 
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
# abb hum key\value pair ko flip kardenge takki uske baad key hamri numbers ban jaye and value words then sortByKey() numbers ko sort kardega  
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count) # t\t means couple of tab space 
