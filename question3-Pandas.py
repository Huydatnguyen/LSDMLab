import sys
import pandas as pd
import time
import glob

# Question 3 Pandas____________________________________________________________start

# start timer
start = time.time()

# getting csv files from the folder 
job_path = "E:\Master2_Semester1\LargeScaleDataManagement\LabGraded\Resource\Job_events"
task_path = "E:\Master2_Semester1\LargeScaleDataManagement\LabGraded\Resource\Task_events"

# refer to the path containing all the files in job/task events table with extension .csv
job_files = glob.glob(job_path + "\*.csv")
task_files = glob.glob(task_path + "\*.csv")

# read all the files in job events table
job_df = pd.concat((pd.read_csv(f, names=["timeStamp","missingInfo","jobID","eventType"
,"userName","schedulingClass","jobName","logicJobName"]) for f in job_files), axis = 0, ignore_index=True)

# read all the files in task events table with specific necessary columns
task_df = pd.concat((pd.read_csv(f, names=["timeStamp","missingInfo","jobID","taskIndex","machineID","eventType"
,"userName","schedulingClass","priority","cpuRequest","ramRequest","diskRequest","constraint"] , usecols=["jobID","taskIndex","schedulingClass"]) for f in task_files), axis = 0, ignore_index=True)

# compute sum of jobs/tasks
print ("job size: " , job_df.count(), "  " , job_df.info(verbose=False))
print ("task size: " , task_df.count(), "  " , task_df.info(verbose=False))

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
