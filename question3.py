import sys
import random
from pyspark import SparkContext
import time
from definition import *

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Question 3____________________________________________________________start

# read the input file into an RDD[String]
task_events_file_0_RDD = sc.textFile("./Task_events/part-00000-of-00500.csv")

# sum of elements(machines)
sum_of_elements = task_events_file_0_RDD.count()

# transformation to a new RDD with spliting each line into an array of items
task_events_file_0_RDD = task_events_file_0_RDD.map(lambda x: x.split(','))

# transformation to a new RDD with each line has only the jobID field
jobID_RDD = task_events_file_0_RDD.map(lambda x: x[Task_events_table.JOB_ID])

# return all of elements of the dataset as a list
jobID_list_full = jobID_RDD.collect()

#print("job_list_full size:  " , len(jobID_list_full))

# remove duplicate 
jobID_list_distinct = list(dict.fromkeys(jobID_list_full))

print("job_list_distinct size:  " , len(jobID_list_distinct))

# for testing
#jobID_list_distinct = ['3418324','3418329','3418334','3418339']

# sampling 
nb_of_samples = 20

jobID_list_sample = random.sample(jobID_list_distinct, nb_of_samples)
print("jobID_list_sample :  " , jobID_list_sample)

# variable represents number of jobs containing tasks running same machine
nb_of_job_satisfied = 0;

# iterator all the elements in the list
for elem in jobID_list_sample:
    # check if job contains only one task
    if jobID_list_full.count(elem) == 1:
       # remove from list
       jobID_list_sample.remove(elem)
       print("job has only one task:  ", elem)
    # if job contains more than one task
    else:
       # filter elements having corresponding jobID
       task_filter_RDD = task_events_file_0_RDD.filter(lambda x: x[Task_events_table.JOB_ID] == elem)
       # list contains machineIDs corresponding to this jobID
       machineID_list = task_filter_RDD.map(lambda x: x[Task_events_table.MACHINE_ID]).collect();

       # If all tasks run on same machine then all of machineID values in machineID_list must be equal, meaning that the number of times an element occurs in list must be equal to the length of list
       check_repeated = machineID_list.count(machineID_list[0]) == len(machineID_list)      

       if (check_repeated):    
          print("All machine IDs corresponds to job " , elem, " are equal ")
          nb_of_job_satisfied += 1;
       else:
          print("Machine IDs corresponds to job " , elem, " are not equal ") 

print("nb_of_job_satisfied :  " , nb_of_job_satisfied)   

# Question 3______________________________________________________________end

input("Press Enter to continnnue...")
