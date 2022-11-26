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

# transformation to a new RDD with each line has only the jobID field
jobID_RDD = task_events_RDD_combined.map(lambda x: x[Task_events_table.JOB_ID])

# return all of elements of the dataset as a list
jobID_list_full = jobID_RDD.collect()

# remove duplicate 
jobID_list_distinct = list(dict.fromkeys(jobID_list_full))

#print("job_list_distinct size:  " , len(jobID_list_distinct))

# sampling 
nb_of_samples = 20

#list contains sampling randomly from list of all jobs
jobID_list_sample = random.sample(jobID_list_distinct, nb_of_samples)

# for testing
#jobID_list_sample = ['3418324','3418329','3418334','5500224185', '3996132741']

#print("jobID_list_sample :  " , jobID_list_sample)

# variable represents number of jobs containing tasks running same machine
nb_of_jobs_satisfied = 0;

# variable represents number of jobs containing only 1 task 
nb_of_jobs_one_task = 0;

# variable represents number of jobs missing resource information 
nb_of_jobs_missing_info = 0;

# iterator all the elements in the list
for elem in jobID_list_sample: 
   
    # check if job contains only one task

    # filter elements having corresponding jobID
    task_filter_RDD = task_events_RDD_combined.filter(lambda x: x[Task_events_table.JOB_ID] == elem)

    # list contains task indexs corresponding to this jobID
    task_index_list = task_filter_RDD.map(lambda x: x[Task_events_table.TASK_INDEX]).collect()

    # If a job contains only one task, meaning that the number of times this task index occurs in list must be equal to the length of list
    check_task_id_repeated = task_index_list.count(task_index_list[0]) == len(task_index_list)   

    # check if job contains only one task
    if (check_task_id_repeated): 
       nb_of_jobs_one_task += 1

    # if job contains more than one task
    else:     
       # list contains CPU request values corresponding to this jobID
       cpuRequest_list = task_filter_RDD.map(lambda x: x[Task_events_table.CPU_REQUEST]).collect()

       # list contains MEMORY request values corresponding to this jobID
       memRequest_list = task_filter_RDD.map(lambda x: x[Task_events_table.MEMORY_REQUEST]).collect()

       # ignore elements missing infor about cpu or memomy request
       if("" in cpuRequest_list or "" in memRequest_list):
          nb_of_jobs_missing_info += 1;
 
       else:
          # If all tasks from a job request the same CPU and MEMORY then all of elements in cpuRequest_list must be equal, meaning that the number of times an element occurs in list must be equal to the length of list
          check_cpuRequest_repeated = cpuRequest_list.count(cpuRequest_list[0]) == len(cpuRequest_list)   
   
          # the same as memRequest_list
          check_memRequest_repeated = memRequest_list.count(memRequest_list[0]) == len(memRequest_list)    

          # check if jobs satisfiedly contain tasks has the same resources 
          if (check_cpuRequest_repeated and check_memRequest_repeated):    
             nb_of_jobs_satisfied += 1

print("nb_of_jobs_with_only_one_task :  " , nb_of_jobs_one_task)   
print("nb_of_jobs_satisfied :  " , nb_of_jobs_satisfied) 
print("nb_of_jobs_missing_info :  " , nb_of_jobs_missing_info) 
print("Percentage of jobs contains tasks having the same resources: ",  round(nb_of_jobs_satisfied /(nb_of_samples - nb_of_jobs_one_task - nb_of_jobs_missing_info) * 100 , 2) , "%" )

# end timer
end = time.time()

print("elapsed time:  " , end-start)

# Question 8______________________________________________________________end

input("Press Enter to continnnue...")
