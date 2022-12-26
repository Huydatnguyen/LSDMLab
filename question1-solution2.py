import sys
from pyspark import SparkContext
import time
from definition import *

# start timer
start = time.time()

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Question 1 solution 2____________________________________________________________start

# read all the input files into an RDD[String]
machine_events_RDD = sc.textFile("./Machine_events/*")

# sum of elements(machines)
sum_of_machines = machine_events_RDD.count()

# transformation to a new RDD with spliting each line into an array of items
machine_events_RDD = machine_events_RDD.map(lambda x: x.split(','))

# transformation to a new RDD with each line contains a <the CPU capacity,1> pair
cpu_capacity_RDD = machine_events_RDD.map(lambda x: (x[Machine_events_table.CPU_CAPACITY],1))

# transformation to a new RDD with merging the values for each key using reduce function
reduce_cpu_capacity_RDD  = cpu_capacity_RDD.reduceByKey(lambda x,y: x+y)

# return as a dictionary
dict_cpu_capacity = dict(reduce_cpu_capacity_RDD.collect())

# iterate each element in dictionary
for key in dict_cpu_capacity:
    # empty key is not valid 
    if key != '':
        print("Percentage of machines correspond with CPU capacity =", key ,"is", round(dict_cpu_capacity[key]/sum_of_machines * 100 , 2) , "%")


# end timer
end = time.time()
print("elapsed time:  " , end-start)

# Question 1 solution 2______________________________________________________________end

input("Press Enter to continnnue...")
