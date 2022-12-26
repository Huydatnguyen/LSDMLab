import sys
import random
from pyspark import SparkContext
import time
from definition import *

# start timer
start = time.time()

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Question 8____________________________________________________________start

# load all files from table and return an RDD[String]
task_events_RDD_combined = sc.textFile("./Task_events/*")

# transformation to a new RDD with spliting each line into an array of items
task_events_RDD_combined = task_events_RDD_combined.map(lambda x: x.split(','))

# transformation to a new RDD with each line is a pair of the priority and cpu request
# with removing elements having the empty value of cpu request
priority_CPU_RDD = task_events_RDD_combined.map(lambda x: (x[Task_events_table.PRIORITY],x[Task_events_table.CPU_REQUEST])).filter(lambda x: x[1] != '')

# countByKey() return a hashmap with the count of each key for CPU request
# after that convert to a dictionary
dict_CPU_countByPriority = dict(priority_CPU_RDD.countByKey())

# transformation to a new RDD with merging the values of CPU request for each key using reduce function
reduce_prio_CPU_RDD = priority_CPU_RDD.reduceByKey(lambda a,b: float(a)+float(b))
# and return as a dictionary
dict_priority_CPU = dict(reduce_prio_CPU_RDD.collect())

# iterate each element in dictionary of priority and CPU
for key in dict_priority_CPU:
    if key != '':
        print("CPU Average corresponds to prior: " , key ," is ", round(dict_priority_CPU[key]/dict_CPU_countByPriority[key],5))
        
        
# transformation to a new RDD with each line is a pair of the priority and memory request
# with removing elements having the empty value of memory request
priority_memory_RDD = task_events_RDD_combined.map(lambda x: (x[Task_events_table.PRIORITY],x[Task_events_table.MEMORY_REQUEST])).filter(lambda x: x[1] != '')
                                                                                                                            # countByKey() return a hashmap with the count of each key for MEMORY request
# after that convert to a dictionary
dict_memory_countByPriority = dict(priority_memory_RDD.countByKey())

# transformation to a new RDD with merging the values of MEMORY request for each key using reduce function
reduce_prio_memory_RDD = priority_memory_RDD.reduceByKey(lambda a,b: float(a)+float(b))
# and return as a dictionary
dict_priority_memory = dict(reduce_prio_memory_RDD.collect())
       
# iterate each element in dictionary of priority and MEMORY
for key in dict_priority_memory:
    if key != '':
        print("MEMORY Average corresponds to prior: " , key ," is ", round(dict_priority_memory[key]/dict_memory_countByPriority[key],5))

# end timer
end = time.time()

print("elapsed time:  " , end-start)

# Question 8______________________________________________________________end

input("Press Enter to continnnue...")
