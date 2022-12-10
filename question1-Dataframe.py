import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import time

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Question 1 Dataframe Spark____________________________________________________________start

# start timer
start = time.time()

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# read all the input files into an Dataframe
df = spark.read.csv("./Machine_events/*")

# rename the Dataframe
newColumns = ["timeStamp","machineID","eventType","platformID","cpu","memory"]
df = df.toDF(*newColumns)

# collect the identical data into groups and count them and show the result
df.groupBy("cpu").count().show(truncate=False)

# end timer
end = time.time()
print("elapsed time:  " , end-start)

# Question 1 Dataframe Spark______________________________________________________________end

input("Press Enter to continnnue...")
