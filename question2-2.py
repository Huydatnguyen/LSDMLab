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

# transformation to a new RDD with each line has only the scheduling class field
scheduling_class_RDD = task_events_RDD_combined.map(lambda x: x[Task_events_table.SCHEDULING_CLASS])

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
       print("Percentage of tasks correspond with scheduling class =", elem ,"is", round(count/sum_of_elements * 100 , 2) , "%  " , count,"of",sum_of_elements)

# end timer
end = time.time()

print("elapsed time:  " , end-start)
# Question 2______________________________________________________________end

input("Press Enter to continnnue...")
