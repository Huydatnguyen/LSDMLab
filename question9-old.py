import sys
import random
from pyspark import SparkContext
import time
from definition import *
from functools import reduce

# start timer
start = time.time()

# Finds out the average of elements in the list lst
def computeAverage(lst):
	return reduce(lambda a, b: a + b, lst) / len(lst)

# convert string value of CPU request to float
def convertCpuValueToFloat(lst):
   # ignore elements having missing infor about CPU request
   if lst[Task_events_table.CPU_REQUEST] != "" :
      return float(lst[Task_events_table.CPU_REQUEST])
   else:
      return "";

# convert string value of MEMORY request to float
def convertMemValueToFloat(lst):
   # ignore elements having missing infor about MEM request
   if lst[Task_events_table.MEMORY_REQUEST] != "" :
      return float(lst[Task_events_table.MEMORY_REQUEST])
   else:
      return "";

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Question 9____________________________________________________________start

# number of files in table
nb_of_files = 2;

# declare an empty RDD for containing data from all files of a table
task_events_RDD_combined = sc.parallelize([])

# read all of input files into an RDD[String]
for i in range(nb_of_files):
   task_events_RDD = sc.textFile("./Task_events/part-00" + standardizeToStr(i) + "-of-00500.csv")
   task_events_RDD_combined = task_events_RDD_combined.union(task_events_RDD)

# transformation to a new RDD with spliting each line into an array of items
task_events_RDD_combined = task_events_RDD_combined.map(lambda x: x.split(','))

# transformation to a new RDD with each line has only the priority field
priority_RDD = task_events_RDD_combined.map(lambda x: x[Task_events_table.PRIORITY])

# return all of elements of the dataset as a list
priority_list_full = priority_RDD.collect()

# remove duplicate 
priority_list_distinct = list(dict.fromkeys(priority_list_full))

print("priority_list_distinct: " , priority_list_distinct)

# iterator all the elements in the list
for elem in priority_list_distinct:

    # filter elements having corresponding this priority
    prior_filter_RDD = task_events_RDD_combined.filter(lambda x: x[Task_events_table.PRIORITY] == elem)

    # list contains CPU request corresponding to this priority 
    cpu_request_list = prior_filter_RDD.map(convertCpuValueToFloat).collect()

    # remove elements having missing CPU request infor
    cpu_request_list = [i for i in cpu_request_list if i != ""]

    # compute the average of CPU request values
    print("CPU Average corresponds to prior: " , elem ," is ", computeAverage(cpu_request_list))

    # list contains MEMORY request corresponding to this priority 
    mem_request_list = prior_filter_RDD.map(convertMemValueToFloat).collect()
  
    # remove elements having missing CPU request infor
    mem_request_list = [i for i in mem_request_list if i != ""]

    # compute the average of MEMORY request values
    print("MEMORY Average corresponds to prior: " , elem ," is ", computeAverage(mem_request_list))

# end timer
end = time.time()

print("elapsed time:  " , end-start)

# Question 9______________________________________________________________end

input("Press Enter to continnnue...")
