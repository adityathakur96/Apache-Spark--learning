# exercise done succesfully khudd karra hai logiv khud karra hai bs code pichle examples ka le liya 



from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmount")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]),float(fields[2]))

input = lines.map(parseLine)

data=input.reduceByKey(lambda x,y:x+y)

results= data.collect()

for result in results:
     print(result[0] + "\t{:.2f}".format(result[1]))
