import sys
from pyspark import SparkContext
import time
from definition import *

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Question 3____________________________________________________________start

# start timer
start = time.time()

# number of files in table
nb_of_files = 2;

# declare an empty RDD for containing data from all files of a table
task_events_RDD_combined = sc.parallelize([])

# read all of input files into an RDD[String]
for i in range(nb_of_files):
    task_events_RDD = sc.textFile("./Task_events/part-00" + standardizeToStr(i) + "-of-00500.csv")
    task_events_RDD_combined = task_events_RDD_combined.union(task_events_RDD)

# sum of elements(machines)
sum_of_elements = task_events_RDD_combined.count()

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
        print("Percentage of tasks correspond with scheduling class =", key ,"is", round(dict_scheduling_class[key]/sum_of_elements * 100 , 2) ,
         "%  " , dict_scheduling_class[key],"of",sum_of_elements)


# end timer
end = time.time()

print("elapsed time:  " , end-start)
# Question 3______________________________________________________________end

input("Press Enter to continnnue...")
