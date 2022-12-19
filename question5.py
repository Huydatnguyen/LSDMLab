import sys
import random
from pyspark import SparkContext
import time
from definition import *

# get input from keyboard
inputVal = input("Please enter the number of samples, or press ENTER to quit: ")
if inputVal!="" :
    inputVal=int(inputVal)

    # start timer
    start = time.time()

    # start spark with 1 worker thread
    sc = SparkContext("local[1]")
    sc.setLogLevel("ERROR")

    # Question 5____________________________________________________________start

    # load all files from table and return an RDD[String]
    task_events_RDD_combined = sc.textFile("./Task_events/*")

    # transformation to a new RDD with spliting each line into an array of items
    task_events_RDD_combined = task_events_RDD_combined.map(lambda x: x.split(','))

    # transformation to a new RDD with each line has only the jobID field
    jobID_taskID_RDD = task_events_RDD_combined.map(lambda x: (x[Task_events_table.JOB_ID] , x[Task_events_table.TASK_INDEX]))

    # remove duplicate 
    jobID_taskID_RDD_distinct = jobID_taskID_RDD.distinct()

    # countByKey() return a hashmap with the count of each key 
    # after that convert to a dictionary
    dict_jobID_taskID = dict(jobID_taskID_RDD_distinct.countByKey())

    # list contains all of jobIDs distinct   
    jobID_list_distinct = task_events_RDD_combined.map(lambda x: (x[Task_events_table.JOB_ID])).distinct().collect()

    # sampling 
    nb_of_samples = inputVal

    #list contains sampling randomly from list of all jobs
    jobID_list_sampling = random.sample(jobID_list_distinct, nb_of_samples)

    # variable represents number of jobs containing tasks running same machine
    nb_of_jobs_satisfied = 0;

    # variable represents number of jobs containing only 1 task 
    nb_of_jobs_one_task = 0;

    # iterate all the elements in the list
    for elem in jobID_list_sampling:
        
        # check if job contains only one task
        if dict_jobID_taskID[elem] == 1:
            nb_of_jobs_one_task += 1
            
        # if job contains more than one task
        else:
            # filter elements from the initial RDD having corresponding jobID
            task_filter_RDD = task_events_RDD_combined.filter(lambda x: x[Task_events_table.JOB_ID] == elem)
        
            # list contains machineIDs corresponding to this jobID
            machineID_list = task_filter_RDD.map(lambda x: x[Task_events_table.MACHINE_ID]).collect()

           # If all tasks run on same machine then all of machineID values in machineID_list must be equal, meaning that the number of times an element occurs in list must be equal to the length of list
            check_machineID_repeated = machineID_list.count(machineID_list[0]) == len(machineID_list)      

            if (check_machineID_repeated):    
                nb_of_jobs_satisfied += 1

    print("nb_of_jobs_with_only_one_task :  " , nb_of_jobs_one_task,"/",nb_of_samples)   
    print("nb_of_jobs_satisfied :  " , nb_of_jobs_satisfied,"/",nb_of_samples) 
    print("Percentage of jobs contains tasks running on the same machines: ",  round(nb_of_jobs_satisfied /(nb_of_samples - nb_of_jobs_one_task) * 100 , 2) , "%" )

    # end timer
    end = time.time()

    print("elapsed time:  " , end-start)

    # Question 5______________________________________________________________end

    input("Press Enter to continnnue...")
