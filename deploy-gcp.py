import os
import time
from pyspark import SparkContext
import matplotlib.pyplot as plt
import numpy as np
from google.cloud import storage

def substract_arrays(x):
	x_list= [x[0][i]-x[1][i] for i in range(len(x[0]))]
	x=tuple(x_list)
	return x

def main():
	# start spark with 1 worker thread
	sc = SparkContext("local[1]")
	sc.setLogLevel("ERROR")

	storage_client = storage.Client()
	bucket = storage_client.get_bucket("fourth-gantry-310809")
	files= [file.name for file in storage_client.list_blobs(bucket)]
	machine_attributes_table=[f for f in files if 'machine_attributes/' in f]
	task_events_table=[f for f in files if 'task_events/' in f]
	resource_usage_table=[f for f in files if 'resource_usage/' in f]

	start_time=time.time()

	# 2. What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?
	
	# read the input file into an RDD[String], there is only one file in the dataset
	machine_events = sc.textFile("gs://fourth-gantry-310809/LSDM/machine_events/part-00000-of-00001.csv")

	# split each line into an array of items
	machine_events = machine_events.map(lambda x : x.split(','))

	# keep the RDD in memory
	machine_events.cache()

	offline_machines=machine_events.filter(lambda m: m[2]=='1').map(lambda m: (m[1],int(m[0]))).groupByKey().map(lambda m: (m[0],sorted(m[1]) ))# get (machine ID, sorted disconnection timestamps) since a machine can disconnect several times
	offline_IDs=offline_machines.keys().collect()
	first_disc_tmps=offline_machines.map(lambda o:(o[0],(o[1][0],len(o[1])))).collectAsMap() # get for each machine its first disconection timestamp for comparison with reconnection, as well as the number of disconnections
	
	#Find reconnected machines with their reconnection timestamps
	reconnected_machines=machine_events.filter(lambda me: me[1] in offline_IDs and int(me[0])>first_disc_tmps[me[1]][0] and me[2]=='0').map(lambda m: (m[1],int(m[0]))).groupByKey().map(lambda m: (m[0],sorted(m[1])[:first_disc_tmps[m[0]][1]])) # get (machine ID, sorted reconnection timestamps of same size as disconnections array)
	rec_IDs=[int(id) for id in reconnected_machines.keys().collect()]
	disconnection_time=reconnected_machines.join(offline_machines).map(lambda x: (x[0],substract_arrays(x[1]))) # get disconnection times for all reconnected machines (machine ID, disconnection time periods)
	#Note: We tried to use foreach(do substruction) but we always get None

	# Calculate total CPU power loss for all machines and then sum to get the total CPU power loss on the cluster
	cpu_capacities= machine_events.filter(lambda m: int(m[1]) in rec_IDs and m[4]!='' and m[5]!='').map(lambda m: (m[1],float(m[4]))).groupByKey().map(lambda m: (m[0],max(list(m[1]))))
	cpu_losses=disconnection_time.join(cpu_capacities).map(lambda c: sum([c[1][1]*x for x in c[1][0]])).collect()
	#print('CPU losses on different machines due to maintenance are:',cpu_losses)
	print('Total CPU loss on the Google cluster due to maintenance is:',sum(cpu_losses))

	# Calculate total memory power loss for all machines and then sum to get the total memory power loss on the cluster
	memory_capacities= machine_events.filter(lambda m: int(m[1]) in rec_IDs and m[4]!='' and m[5]!='').map(lambda m: (m[1],float(m[5]))).groupByKey().map(lambda m: (m[0],max(list(m[1]))))
	memory_losses=disconnection_time.join(memory_capacities).map(lambda c: sum([c[1][1]*x for x in c[1][0]])).collect()
	#print('Memory losses on different machines due to maintenance are:',memory_losses)
	print('Total memory loss on the Google cluster due to maintenance is:',sum(memory_losses))
	
	
	# 4. Do tasks with a low scheduling class have a higher probability of being evicted?

	# read the input files into an RDD[String]
	task_events = sc.parallelize([])
	#files=list(os.system("gsutil ls -a gs://fourth-gantry-310809/LSDM/task_events"))
	
	for file in task_events_table:
		task_events_rdd = sc.textFile("gs://fourth-gantry-310809/"+file)
		task_events = task_events.union(task_events_rdd)

	# split each line into an array of items
	task_events = task_events.map(lambda x : x.split(','))

	# keep the RDD in memory
	task_events.cache()
	# filter data by leaving only evicted tasks
	filtered = task_events.filter(lambda t: t[5]=='2')
	size=filtered.count()

	# map to each scheduling class the portion of its evicted tasks
	pairs = filtered.map(lambda f: (f[7],1/size)).reduceByKey(lambda a, b: (a + b))
	pairs=pairs.sortByKey().collectAsMap()
	print(pairs)
			

	# 6. Are the tasks that request the more resources the ones that consume the more resources?
	
	# read the input files into an RDD[String]
	task_events = sc.parallelize([])
	for file in task_events_table:
		task_events_rdd = sc.textFile("gs://fourth-gantry-310809/"+file)
		task_events = task_events.union(task_events_rdd)

	# split each line into an array of items
	task_events = task_events.map(lambda x : x.split(','))

	# keep the RDD in memory
	task_events.cache()
	
	filtered=task_events.filter(lambda t: t[9]!='' and t[10]!='' and t[11]!='') # remove tasks whose resource requests are not specified
	# extract tasks that request big resources (CPU, RAM, local disk resource request > 0.5)
	tasks_big=filtered.filter(lambda t: float(t[9])>0.5 or float(t[10])>0.5 or float(t[11])>0.5) 
	tasks_big=tasks_big.map(lambda t: (t[2]+t[3],1)).distinct()
	# print('requests: ',tasks_big)

	# read the input files into an RDD[String]
	resource_usage = sc.parallelize([])
	for file in resource_usage_table:
		rsc_usage_rdd = sc.textFile("gs://fourth-gantry-310809/"+file)
		resource_usage = resource_usage.union(rsc_usage_rdd)
	# split each line into an array of items
	resource_usage = resource_usage.map(lambda x : x.split(','))

	# keep the RDD in memory
	resource_usage.cache()

	filtered_2=resource_usage.filter(lambda t: t[5]!='' and t[6]!='' and t[12]!='')
	# extract tasks that consume big resources
	rsc_big=filtered_2.filter(lambda t: float(t[5])>0.5 or float(t[6])>0.5 or float(t[12])>0.5)
	rsc_big=rsc_big.map(lambda t: (t[2]+t[3],1)).distinct()
	# print('usage: ',rsc_big)

	nb_tasks_big_rsc=rsc_big.join(tasks_big).count()
	print("Percentage of tasks consuming big resources among those requesting big resources: ",nb_tasks_big_rsc/rsc_big.count())


	# 7. Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
			
	# read the input files into an RDD[String]
	resource_usage = sc.parallelize([])
	for file in resource_usage_table:
		rsc_usage_rdd = sc.textFile("gs://fourth-gantry-310809/"+file)
		resource_usage = resource_usage.union(rsc_usage_rdd)
	# split each line into an array of items
	resource_usage = resource_usage.map(lambda x : x.split(','))
	# keep the RDD in memory
	resource_usage.cache()

	# read the input files into an RDD[String]
	task_events = sc.parallelize([])
	for file in task_events_table:
		task_events_rdd = sc.textFile("gs://fourth-gantry-310809/"+file)
		task_events = task_events.union(task_events_rdd)
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
	print('Max total CPU usage per task: ',joined_rdd)

	## b) Memory consumption
	tasks_max_mem=tasks.join(tasks_mem_usage).map(lambda t: (t[0],t[1][1]))# get (taskID, max memory usage)
	machines=tasks_max_mem.join(tasks).map(lambda t: (t[1][1][0],t[1][0])) # get(machine ID, max task mem usage)
	total_mem_usg_per_machine=machines.reduceByKey(lambda a, b: a + b)# get (machine ID, total max memory usage))

	joined_rdd=events.join(total_mem_usg_per_machine).map(lambda t: (t[1][0],t[1][1])).reduceByKey(max).sortByKey().collect()# get (event type, total max memory usage)
	print('Max total Memory usage per task: ',joined_rdd)

	## c) Local disk
	tasks_max_disk=tasks.join(tasks_ldisk_usage).map(lambda t: (t[0],t[1][1]))# get (taskID, max local disk usage)
	machines=tasks_max_disk.join(tasks).map(lambda t: (t[1][1][0],t[1][0])) # get(machine ID, max task local disk usage)
	total_ld_usg_per_machine=machines.reduceByKey(lambda a, b: a + b)# get (machine ID, total local disk usage))

	joined_rdd=events.join(total_ld_usg_per_machine).map(lambda t: (t[1][0],t[1][1])).reduceByKey(max).sortByKey().collect()# get (event type, total local disk usage)
	print('Max total Local Disk usage per task: ',joined_rdd)
		

	# 9. What are hardware specifications of machines on which different priority tasks have/haven't successfully run?
			
	# read the input files into an RDD[String]
	task_events = sc.parallelize([])
	for file in task_events_table:
		task_events_rdd = sc.textFile("gs://fourth-gantry-310809/"+file)
		task_events = task_events.union(task_events_rdd)
	# split each line into an array of items
	task_events = task_events.map(lambda x : x.split(','))
	# keep the RDD in memory
	task_events.cache()

	# create machine attributes RDD
	machine_attributes = sc.parallelize([])
	for file in machine_attributes_table:
		machine_att_rdd = sc.textFile("gs://fourth-gantry-310809/"+file)
		machine_attributes = machine_attributes.union(machine_att_rdd)
	machine_attributes = machine_attributes.map(lambda x : x.split(','))
	machine_attributes = machine_attributes.map(lambda m: (m[1],(m[2],m[3])))
	machine_attributes.cache()

	# get priorities of finished and failed events (no need for the other event types since we're not interested in)
	priorities=task_events.filter(lambda t: t[5]=='3' or t[5]=='4').map(lambda t: t[8]).distinct().collect()
	for priority in priorities:
		# extract hosting machines of all finished and failed tasks
		machine_id_finished_tasks=task_events.filter(lambda t:t[5]=='4' and t[8]==priority).map(lambda t: (t[4],1)).distinct()
		machine_id_failed_tasks=task_events.filter(lambda t:t[5]=='3' and t[8]==priority).map(lambda t: (t[4],1)).distinct()
		
		# note that the attribute name is an opaque string and the attribute value could be either an opaque string or an integer
		hardware_finished=machine_attributes.join(machine_id_finished_tasks).map(lambda m: (m[0],(m[1][0][0],m[1][0][1]))).collect()
		hardware_failed=machine_attributes.join(machine_id_failed_tasks).map(lambda m: (m[0],(m[1][0][0],m[1][0][1]))).collect()
		# print('Hardware characteristics for machines that have successfully run tasks with priority {} are: {}'.format(priority,hardware_finished))
		# print('Hardware characteristics for machines that have failed to run tasks with priority {} are: {}'.format(priority,hardware_failed))

	end_time=time.time() - start_time
	print('Processing time: ',end_time)
	
if __name__=="__main__":
	main()