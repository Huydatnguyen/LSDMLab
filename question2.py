import sys
from pyspark import SparkContext
import time
from definition import *

# start timer
start = time.time()

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Question 2____________________________________________________________start

# read the input file into an RDD[String]
job_events_file_0_RDD = sc.textFile("./Job_events/part-00000-of-00500.csv")

# sum of elements(machines)
sum_of_elements = job_events_file_0_RDD.count()

# transformation to a new RDD with spliting each line into an array of items
job_events_file_0_RDD = job_events_file_0_RDD.map(lambda x: x.split(','))

# transformation to a new RDD with each line has only the scheduling class field
scheduling_class_RDD = job_events_file_0_RDD.map(lambda x: x[Job_events_table.SCHEDULING_CLASS])

# return all of elements of the dataset as a list
scheduling_class_list = scheduling_class_RDD.collect()

# remove duplicate 
scheduling_class_list = list(dict.fromkeys(scheduling_class_list))

print("scheduling_class_list ", scheduling_class_list)

# iterator all the elements in the list
for elem in scheduling_class_list:
    #ignore the empty value
    if elem != '':
       # filter all elements corresponding with 'elem' value in the list and count them
       count = scheduling_class_RDD.filter(lambda x: x==elem).count()
       print("Percentage of jobs correspond with scheduling class =", elem ,"is", round(count/sum_of_elements * 100 , 2) , "%  " , count)

# end timer
end = time.time()

print("elapsed time:  " , end-start)

# Question 2______________________________________________________________end

input("Press Enter to continnnue...")
