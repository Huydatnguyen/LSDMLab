import sys
from pyspark import SparkContext
import time
from definition import *

# Question 3 for job events table____________________________________________________________start

# start timer
start = time.time()

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# load all files from table and return an RDD[String]
job_events_RDD_combined = sc.textFile("./Job_events/*")
task_events_RDD_combined = sc.textFile("./Task_events/*")

# sum of jobs
sum_of_jobs = int(job_events_RDD_combined.count())

# transformation to a new RDD with spliting each line into an array of items
job_events_RDD_combined = job_events_RDD_combined.map(lambda x: x.split(','))

# transformation to a new RDD with each line contains a <the scheduling_class,1> pair
scheduling_class_RDD = job_events_RDD_combined.map(lambda x: (x[Job_events_table.SCHEDULING_CLASS],1))

# return a hashmap with the count of each key
hashmap_scheduling_class = scheduling_class_RDD.countByKey()

# return as a dictionary
dict_scheduling_class = dict(hashmap_scheduling_class)

# iterate each element in dictionary
for key in dict_scheduling_class:
    # empty key is not valid 
    if key != '':
        print("Percentage of jobs correspond with scheduling class =", key ,"is", 
round(dict_scheduling_class[key]/sum_of_jobs * 100 , 2) ,
         "%  " , dict_scheduling_class[key],"of",sum_of_jobs)

# Question 3 for job events table______________________________________________________________end


# Question 3 for task events table____________________________________________________________start

# sum of tasks
sum_of_tasks = int(task_events_RDD_combined.count())

# transformation to a new RDD with spliting each line into an array of items
task_events_RDD_combined = task_events_RDD_combined.map(lambda x: x.split(','))

# transformation to a new RDD with each line contains a <the scheduling_class,1> pair
scheduling_class_RDD = task_events_RDD_combined.map(lambda x: (x[Task_events_table.SCHEDULING_CLASS],1))

# transformation to a new RDD with merging the values for each key using reduce function
reduce_scheduling_class_RDD  = scheduling_class_RDD.reduceByKey(lambda x,y: x+y)

# return as a dictionary
dict_scheduling_class = dict(reduce_scheduling_class_RDD.collect())

# iterate each element in dictionary
for key in dict_scheduling_class:
    # empty key is not valid 
    if key != '':
        print("Percentage of tasks correspond with scheduling class =", key ,"is", round(dict_scheduling_class[key]/sum_of_tasks * 100 , 2) ,
         "%  " , dict_scheduling_class[key],"of",sum_of_tasks)  

# Question 3 for task events table______________________________________________________________end   

# end timer
end = time.time()

print("elapsed time:  " , end-start)

input("Press Enter to continnnue...")
