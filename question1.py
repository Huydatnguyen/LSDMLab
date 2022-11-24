import sys
from pyspark import SparkContext
import time
from definition import *

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Question 1____________________________________________________________start

# read the input file into an RDD[String]
machine_events_RDD = sc.textFile("./Machine_events/part-00000-of-00001.csv")

# sum of elements(machines)
sum_of_machines = machine_events_RDD.count()

# transformation to a new RDD with spliting each line into an array of items
machine_events_RDD = machine_events_RDD.map(lambda x: x.split(','))

# transformation to a new RDD with each line has only the CPU capacity field
cpu_capacity_RDD = machine_events_RDD.map(lambda x: x[Machine_events_table.CPU_CAPACITY])

""" ____need to be checked
# use distinct() func of Spark to remove duplicated elements
CPU_capacity_RDD = CPU_capacity_RDD.distinct()

# # return all of elements after removing duplicates as a list
cpu_capacity_list = CPU_capacity_RDD.collect()
"""

# return all of elements of the dataset as a list
cpu_capacity_list = cpu_capacity_RDD.collect()

# remove duplicate 
cpu_capacity_list = list(dict.fromkeys(cpu_capacity_list))

# iterator all the elements in the list
for elem in cpu_capacity_list:
    #ignore the empty value
    if elem != '':
       # filter all elements corresponding with 'elem' value in the list and count them
       count = cpu_capacity_RDD.filter(lambda x: x==elem).count()
       print("Percentage of machines correspond with CPU capacity =", elem ,"is", round(count/sum_of_machines * 100 , 2) , "%")

# Question 1______________________________________________________________end

input("Press Enter to continnnue...")
