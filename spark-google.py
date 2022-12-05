import os
from pyspark import SparkContext
import matplotlib.pyplot as plt
import numpy as np

def substract_arrays(x):
	x_list= [x[0][i]-x[1][i] for i in range(len(x[0]))]
	x=tuple(x_list)
	return x

def main():
	# start spark with 1 worker thread
	sc = SparkContext("local[1]")
	sc.setLogLevel("ERROR")
	val=0
	while val!="":
		val = input("Please enter the number of your question, or press ENTER to quit: ")
		if val=="" :
			break
		else:
			val=int(val)
		
		if val==2:
		# 2. What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?
			# read the input file into an RDD[String], there is only one file in the dataset
			machine_events = sc.textFile("machine_events/part-00000-of-00001.csv")

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
			cpu_capacities= machine_events.filter(lambda m: int(m[1]) in rec_IDs and m[4]!='' and m[5]!='').map(lambda m: (m[1],float(m[4]))).groupByKey().map(lambda m: (m[0],list(m[1])[0]))
			cpu_losses=disconnection_time.join(cpu_capacities).distinct().map(lambda c: sum([c[1][1]*x for x in c[1][0]])).collect()
			#print('CPU losses on different machines due to maintenance are:',cpu_losses)
			print('Total CPU loss on the Google cluster due to maintenance is:',sum(cpu_losses))

			# Calculate total memory power loss for all machines and then sum to get the total memory power loss on the cluster
			memory_capacities= machine_events.filter(lambda m: int(m[1]) in rec_IDs and m[4]!='' and m[5]!='').map(lambda m: (m[1],float(m[5]))).groupByKey().map(lambda m: (m[0],list(m[1])[0]))
			memory_losses=disconnection_time.join(memory_capacities).distinct().map(lambda c: sum([c[1][1]*x for x in c[1][0]])).collect()
			#print('Memory losses on different machines due to maintenance are:',memory_losses)
			print('Total memory loss on the Google cluster due to maintenance is:',sum(memory_losses))
			
			X_axis = np.arange(99)
	
			plt.bar(X_axis - 0.2, cpu_losses[:99], 0.4, label = 'CPU losses')
			plt.bar(X_axis + 0.2, memory_losses[:99], 0.4, label = 'Memory losses')
			
			plt.xticks(X_axis, X_axis)
			plt.xlabel("Maintained machines")
			plt.ylabel("Power loss")
			plt.title("Computational power losses for maintained machines")
			plt.legend()
			plt.show()


		elif val==4:

		# 4. Do tasks with a low scheduling class have a higher probability of being evicted?

			# read the input files into an RDD[String]
			task_events = sc.parallelize([])
			files=os.listdir("task_events/")
			for file in files:
				task_events_rdd = sc.textFile("task_events/"+file)
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
			plt.bar(pairs.keys(),pairs.values(),align='center')
			plt.title('Distribution of evicted tasks over scheduling classes')
			plt.xlabel('scheduling class')
			plt.ylabel('Probability of task eviction')
			plt.show()
			

		elif val==6:

		# 6. Are the tasks that request the more resources the ones that consume the more resources?
			# read the input files into an RDD[String]
			task_events = sc.parallelize([])
			files=os.listdir("task_events/")
			for file in files:
				task_events_rdd = sc.textFile("task_events/"+file)
				task_events = task_events.union(task_events_rdd)

			# split each line into an array of items
			task_events = task_events.map(lambda x : x.split(','))

			# keep the RDD in memory
			task_events.cache()
			filtered=task_events.filter(lambda t: t[9]!='' and t[10]!='' and t[11]!='') # remove tasks whose resource requests are not specified
			# extract tasks that request big resources (CPU, RAM, local disk resource request > 0.5)
			tasks_big=filtered.filter(lambda t: float(t[9])>0.5 or float(t[10])>0.5 or float(t[11])>0.5) 
			tasks_big=tasks_big.map(lambda t: (t[2]+t[3])).distinct()

			# read the input files into an RDD[String]
			resource_usage = sc.parallelize([])
			files=os.listdir("resource_usage/")
			for file in files:
				rsc_usage_rdd = sc.textFile("resource_usage/"+file)
				resource_usage = resource_usage.union(rsc_usage_rdd)

			filtered=resource_usage.filter(lambda t: t[5]!='' and t[6]!='' and t[12]!='')
			# extract tasks that consume big resources
			rsc_big=filtered.filter(lambda t: float(t[5])>0.5 or float(t[6])>0.5 or float(t[12])>0.5).map(lambda t: (t[2]+t[3])).distinct()
			nb_tasks_big_rsc=rsc_big.join(tasks_big).count()
			print("Percentage of tasks consuming big resources among those requesting big resources: ",nb_tasks_big_rsc/rsc_big.count())

		elif val==7:
		# 7. Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
			# read the input files into an RDD[String]
			resource_usage = sc.parallelize([])
			files=os.listdir("resource_usage/")
			for file in files:
				rsc_usage_rdd = sc.textFile("resource_usage/"+file)
				resource_usage = resource_usage.union(rsc_usage_rdd)
			# split each line into an array of items
			resource_usage = resource_usage.map(lambda x : x.split(','))
			# keep the RDD in memory
			resource_usage.cache()

			# read the input files into an RDD[String]
			task_events = sc.parallelize([])
			files=os.listdir("task_events/")
			for file in files:
				task_events_rdd = sc.textFile("task_events/"+file)
				task_events = task_events.union(task_events_rdd)
			# split each line into an array of items
			task_events = task_events.map(lambda x : x.split(','))
			# keep the RDD in memory
			task_events.cache()

			## a) CPU consumption
			filtered=resource_usage.filter(lambda t: t[5]!='' and t[6]!='')
			tasks_cpu_usage=filtered.map(lambda t: (t[2]+t[3],float(t[5]))) #get cpu usage for all tasks
			tasks_mem_usage=filtered.map(lambda t: (t[2]+t[3],float(t[6]))) #get memory usage for all tasks
			tasks_ldisk_usage=filtered.map(lambda t: (t[2]+t[3],float(t[12]))) #get local disk usage for all tasks
			
			filtered=task_events.filter(lambda t: t[4]!='' and t[9]!='' and t[10]!='' and t[11]!='') # clean our data 
			tasks=filtered.map(lambda t: (t[2]+t[3],(t[4],t[5])))
			tasks_max_cpu=tasks.join(tasks_cpu_usage).map(lambda t: (t[0],t[1][1])).reduceByKey(max) # get (taskID, max cpu usage)
			machines=tasks_max_cpu.join(tasks).map(lambda t: (t[1][1][0],t[1][0])) # get(machine ID, max task cpu usage)
			total_cpu_usg_per_machine=machines.reduceByKey(lambda a, b: a + b)# get (machine ID, total cpu usage))
			events=tasks.map(lambda t: (t[1][0],t[1][1])) #get (machine ID, event type)

			joined_rdd=events.join(total_cpu_usg_per_machine).map(lambda t: (t[1][0],t[1][1])).reduceByKey(max).sortByKey().collect()# get (event type, total cpu usage)
			plt.plot(*zip(*joined_rdd))
			plt.title('Max CPU consumption in terms of events types')
			plt.xlabel('event types')
			plt.ylabel('CPU consumption')
			plt.show()

			## b) Memory consumption
			tasks_max_mem=tasks.join(tasks_mem_usage).map(lambda t: (t[0],t[1][1])).reduceByKey(max) # get (taskID, max memory usage)
			machines=tasks_max_mem.join(tasks).map(lambda t: (t[1][1][0],t[1][0])) # get(machine ID, max task mem usage)
			total_mem_usg_per_machine=machines.reduceByKey(lambda a, b: a + b)# get (machine ID, total memory usage))

			joined_rdd=events.join(total_mem_usg_per_machine).map(lambda t: (t[1][0],t[1][1])).reduceByKey(max).sortByKey().collect()# get (event type, total memory usage)
			plt.plot(*zip(*joined_rdd))
			plt.title('Max Memory consumption in terms of events types')
			plt.xlabel('event types')
			plt.ylabel('Memory consumption')
			plt.show()

			## c) Local disk
			tasks_max_disk=tasks.join(tasks_ldisk_usage).map(lambda t: (t[0],t[1][1])).reduceByKey(max) # get (taskID, max local disk usage)
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