import sys
import pandas as pd
import time
import glob

# Question 4 Pandas____________________________________________________________start

# start timer
start = time.time()

# getting csv files from the folder 
task_path = "E:\Master2_Semester1\LargeScaleDataManagement\LabGraded\Resource\Task_events"

# refer to the path containing all the files in job/task events table with extension .csv
task_files = glob.glob(task_path + "\*.csv")

# read all the files in task events table with specific necessary columns
task_df = pd.concat((pd.read_csv(f, names=["timeStamp","missingInfo","jobID","taskIndex","machineID","eventType"
,"userName","schedulingClass","priority","cpuRequest","ramRequest","diskRequest","constraint"] , usecols=["eventType","schedulingClass"]) for f in task_files), axis = 0, ignore_index=True)

# list contains scheduling class values with removing duplicates
list_scheduling_class = task_df['schedulingClass'].unique() 

# iterate each element in list
for elem in list_scheduling_class:
    # empty element is not valid 
    if elem != '':
    	# new dataframe with filtering tasks with corresponding scheduling class  
    	scheduling_class_df = task_df[task_df['schedulingClass'] == elem]

    	# new dataframe with filtering tasks with corresponding scheduling class and evicted
    	evicted_tasks = task_df[(task_df['schedulingClass'] == elem) & (task_df['eventType'] == 2)]
 
 		# compute the result and show 
    	print("Percentage of tasks having scheduling class: " , elem, "which were evicted is: " , round(evicted_tasks.count()/scheduling_class_df.count(),2))

# end timer
end = time.time()
print("elapsed time:  " , end-start)

# Question 4 Pandas______________________________________________________________end

input("Press Enter to continnnue...")
