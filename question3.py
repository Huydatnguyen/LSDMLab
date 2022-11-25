import sys
import random
from pyspark import SparkContext
import time
from definition import *

# start timer
start = time.time()

# start spark with 1 worker thread
sc = SparkContext("local[2]")
sc.setLogLevel("ERROR")

# Question 3____________________________________________________________start

# read the input file into an RDD[String]
task_events_file_0_RDD = sc.textFile("./Task_events/part-00000-of-00500.csv")

# sum of elements(machines)
#sum_of_elements = task_events_file_0_RDD.count()

# transformation to a new RDD with spliting each line into an array of items
task_events_file_0_RDD = task_events_file_0_RDD.map(lambda x: x.split(','))

# transformation to a new RDD with each line has only the jobID field
jobID_RDD = task_events_file_0_RDD.map(lambda x: x[Task_events_table.JOB_ID])

# return all of elements of the dataset as a list
jobID_list_full = jobID_RDD.collect()

# remove duplicate 
jobID_list_distinct = list(dict.fromkeys(jobID_list_full))

#print("job_list_distinct size:  " , len(jobID_list_distinct))

# sampling 
nb_of_samples = 20

jobID_list_sample = random.sample(jobID_list_distinct, nb_of_samples)

# for testing
#jobID_list_sample = ['3418324','3418329','3418334','5500224185', '3996132741']

#print("jobID_list_sample :  " , jobID_list_sample)

# variable represents number of jobs containing tasks running same machine
nb_of_jobs_satisfied = 0;

# variable represents number of jobs containing only 1 task 
nb_of_jobs_one_task = 0;

# iterator all the elements in the list
for elem in jobID_list_sample: 
   
    # check if job contains only one task

    # filter elements having corresponding jobID
    task_filter_RDD = task_events_file_0_RDD.filter(lambda x: x[Task_events_table.JOB_ID] == elem)

    # list contains task indexs corresponding to this jobID
    task_index_list = task_filter_RDD.map(lambda x: x[Task_events_table.TASK_INDEX]).collect()

    # If a job contains only one task, meaning that the number of times this task index occurs in list must be equal to the length of list
    check_task_id_repeated = task_index_list.count(task_index_list[0]) == len(task_index_list)   

    # check if job contains only one task
    if (check_task_id_repeated): 
       print("job has only one task:  ", elem)
       nb_of_jobs_one_task += 1

    # if job contains more than one task
    else:     
       # list contains machineIDs corresponding to this jobID
       machineID_list = task_filter_RDD.map(lambda x: x[Task_events_table.MACHINE_ID]).collect()

       # If all tasks run on same machine then all of machineID values in machineID_list must be equal, meaning that the number of times an element occurs in list must be equal to the length of list
       check_machineID_repeated = machineID_list.count(machineID_list[0]) == len(machineID_list)      

       if (check_machineID_repeated):    
          print("All machine IDs corresponds to job " , elem, " are equal ")
          nb_of_jobs_satisfied += 1
       else:
          print("Machine IDs corresponds to job " , elem, " are not equal ") 

print("nb_of_jobs_with_only_one_task :  " , nb_of_jobs_one_task)   
print("nb_of_jobs_satisfied :  " , nb_of_jobs_satisfied) 

# end timer
end = time.time()

print("elapsed time:  " , end-start)

# Question 3______________________________________________________________end

input("Press Enter to continnnue...")
