import os
from pyspark import SparkContext
import matplotlib.pyplot as plt

def main():
    sc = SparkContext("local[4]")
    sc.setLogLevel("ERROR")
    
    # 7. Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?		
    # read the input files into an RDD[String], 3 files of Task_events table were used. 
    resource_usage = sc.textFile("./Resource_usage/*")
    # split each line into an array of items
    resource_usage = resource_usage.map(lambda x : x.split(','))
    # keep the RDD in memory
    resource_usage.cache()

    # read the input files into an RDD[String], 3 files of Task_events table were used. 
    task_events = task_events = sc.textFile("./Task_events/*")
    # split each line into an array of items
    task_events = task_events.map(lambda x : x.split(','))
    # keep the RDD in memory
    task_events.cache()

    ## a) CPU consumption
    filtered=resource_usage.filter(lambda t: t[5]!='' and t[6]!='' and t[12]!='')
    tasks_cpu_usage=filtered.map(lambda t: (t[2]+t[3],float(t[5]))).reduceByKey(max) #get max cpu usage for all tasks
    tasks_mem_usage=filtered.map(lambda t: (t[2]+t[3],float(t[6]))).reduceByKey(max) #get max memory usage for all tasks
    tasks_ldisk_usage=filtered.map(lambda t: (t[2]+t[3],float(t[12]))).reduceByKey(max) #get max local disk usage for all tasks
    
    filtered=task_events.filter(lambda t: t[4]!='' and t[9]!='' and t[10]!='' and t[11]!='') # clean our data by removing tasks that have missing values for Machine ID, CPU/RAM/local disk requests
    tasks=filtered.map(lambda t: (t[2]+t[3],(t[4],t[5]))) # get (taskID,(machine ID, event type))
    machines=tasks_cpu_usage.join(tasks).map(lambda t: (t[1][1][0],t[1][0])) # get(machine ID, task max cpu usage)
    total_cpu_usg_per_machine=machines.reduceByKey(lambda a, b: a + b)# get (machine ID, max total cpu usage))
    events=tasks.map(lambda t: (t[1][0],t[1][1])) #get (machine ID, event type)

    joined_rdd=events.join(total_cpu_usg_per_machine).map(lambda t: (t[1][0],t[1][1])).reduceByKey(max).sortByKey().collect()# get (event type, max total cpu usage)
    plt.plot(*zip(*joined_rdd))
    plt.title('Max CPU consumption in terms of events types')
    plt.xlabel('event types')
    plt.ylabel('CPU consumption')
    plt.show()

    ## b) Memory consumption
    tasks_max_mem=tasks.join(tasks_mem_usage).map(lambda t: (t[0],t[1][1]))# get (taskID, max memory usage)
    machines=tasks_max_mem.join(tasks).map(lambda t: (t[1][1][0],t[1][0])) # get(machine ID, max task mem usage)
    total_mem_usg_per_machine=machines.reduceByKey(lambda a, b: a + b)# get (machine ID, total max memory usage))

    joined_rdd=events.join(total_mem_usg_per_machine).map(lambda t: (t[1][0],t[1][1])).reduceByKey(max).sortByKey().collect()# get (event type, total max memory usage)
    plt.plot(*zip(*joined_rdd))
    plt.title('Max Memory consumption in terms of events types')
    plt.xlabel('event types')
    plt.ylabel('Memory consumption')
    plt.show()

    ## c) Local disk
    tasks_max_disk=tasks.join(tasks_ldisk_usage).map(lambda t: (t[0],t[1][1]))# get (taskID, max local disk usage)
    machines=tasks_max_disk.join(tasks).map(lambda t: (t[1][1][0],t[1][0])) # get(machine ID, max task local disk usage)
    total_ld_usg_per_machine=machines.reduceByKey(lambda a, b: a + b)# get (machine ID, total local disk usage))

    joined_rdd=events.join(total_ld_usg_per_machine).map(lambda t: (t[1][0],t[1][1])).reduceByKey(max).sortByKey().collect()# get (event type, total local disk usage)
    plt.plot(*zip(*joined_rdd))
    plt.title('Max Local Disk consumption in terms of events types')
    plt.xlabel('event types')
    plt.ylabel('Local Disk consumption')
    plt.show()

if __name__=="__main__":
	main()