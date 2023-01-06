import sys
import pandas as pd
import time
import glob

# Question 3 Pandas____________________________________________________________start

# start timer
start = time.time()

# getting csv files from the folder 
job_path = ".\Job_events"
task_path = ".\Task_events"

# refer to the path containing all the files in job/task events table with extension .csv
job_files = glob.glob(job_path + "\*.csv")
task_files = glob.glob(task_path + "\*.csv")

# read all the files in job events table
job_df = pd.concat((pd.read_csv(f, names=["timeStamp","missingInfo","jobID","eventType"
,"userName","schedulingClass","jobName","logicJobName"]) for f in job_files), axis = 0, ignore_index=True)

# read all the files in task events table with specific necessary columns
task_df = pd.concat((pd.read_csv(f, names=["timeStamp","missingInfo","jobID","taskIndex","machineID","eventType"
,"userName","schedulingClass","priority","cpuRequest","ramRequest","diskRequest","constraint"] , usecols=["jobID","taskIndex","schedulingClass"]) for f in task_files), axis = 0, ignore_index=True)

# collect the identical data about scheduling Class into groups and count them 
job_result = job_df.groupby(['schedulingClass']).count()
task_result = task_df.groupby(['schedulingClass']).count()

# show the result
print(job_result) 
print(task_result) 

# end timer
end = time.time()
print("elapsed time:  " , end-start)

# Question 3 Pandas______________________________________________________________end

input("Press Enter to continnnue...")
