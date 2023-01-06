import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import time

# Question 3 Dataframe Spark____________________________________________________________start

# start timer
start = time.time()

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# init spark application
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# read all the input files into an Dataframe
job_df = spark.read.csv("./Job_events/*")
task_df = spark.read.csv("./Task_events/*")

# rename the Dataframe columns
job_newColumns = ["timeStamp","","jobID","eventType","","schedulingClass","jobName",""]
job_df = job_df.toDF(*job_newColumns)

task_newColumns = ["timeStamp","","jobID","taskIndex","machineID","eventType","","schedulingClass","priority","cpuRequest","ramRequest","diskRequest",""]
task_df = task_df.toDF(*task_newColumns)

# collect the identical data into groups and count them and show the result
job_df.groupBy("schedulingClass").count().show(truncate=False)
task_df.groupBy("schedulingClass").count().show(truncate=False)

# calculate sum of jobs/tasks
print("sum of jobs: ", job_df.count())
print("sum of tasks: ", task_df.count())

# end timer
end = time.time()
print("elapsed time:  " , end-start)

# Question 3 Dataframe Spark______________________________________________________________end

input("Press Enter to continnnue...")
