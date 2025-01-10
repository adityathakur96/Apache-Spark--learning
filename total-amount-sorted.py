# exercise done succesfully khudd karra hai logiv khud karra hai bs code pichle examples ka le liya 



from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountSorted")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]),float(fields[2]))

input = lines.map(parseLine)

data=input.reduceByKey(lambda x,y:x+y)
sortation = data.map(lambda x: (x[1] , x[0])).sortByKey()  # can i be able to do lambda (x,y):(y,x) should check from chat gpt 
results= sortation.collect()

for result in results:
     print( result[1] , "\t{:.2f}".format(result[0]))
