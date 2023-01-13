from pyspark import SparkContext
import matplotlib.pyplot as plt
import numpy as np

def substract_arrays(x):
	x_list= [x[0][i]-x[1][i] for i in range(len(x[0]))]
	x=tuple(x_list)
	return x

def main():
    sc = SparkContext("local[2]")
    sc.setLogLevel("ERROR")
    
    # 2. What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?
    # read the input file into an RDD[String], there is only one file in the dataset
    machine_events = sc.textFile("./Machine_events/*")

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
    cpu_capacities= machine_events.filter(lambda m: int(m[1]) in rec_IDs and m[4]!='' and m[5]!='').map(lambda m: (m[1],float(m[4]))).reduceByKey(max)
    cpu_losses=disconnection_time.join(cpu_capacities).map(lambda c: sum([c[1][1]*x for x in c[1][0]])).collect()
    #print('CPU losses on different machines due to maintenance are:',cpu_losses)
    #In order to calculate the percentage of the cpu loss, we need to calculate the cpu power of the whole cluster which is equal to sum of cpu_capacity_machine_i*connection_period
    #For a simplification reason, we assume that the connection period of a machine is the time difference between the first timestamp of an event which is different from REMOVE
    #the last timestamp in the dataset.
    
    #Get first connection/update timestamp for all machines
    first_connected_occurence=machine_events.filter(lambda m: m[2]!='1').map(lambda m: (m[1],int(m[0]))).reduceByKey(min) #(machine_ID, oldest connected appearance in the cluster)
    #Get last timestamp for all machines
    last_occurence=machine_events.map(lambda m: (m[1],int(m[0]))).reduceByKey(max)#(machine_ID, latest appearance in the cluster)
    total_connection_periods=last_occurence.union(first_connected_occurence).distinct().reduceByKey(lambda a,b:a-b)
    #print(total_connection_periods)
    
    #By visually examining the print results, we found out that there are a lot of machines that their last occurence time was at timestamp 0
    #which means that these machines didn't go off during the whole tracing process.
    #We assign to these machines the latest recorded timestamp in the dataset used
    latest_tmp=max(last_occurence.values().collect())
    all_time_connected_machines=total_connection_periods.filter(lambda machine:machine[1]==0).map(lambda m:(m[0],latest_tmp))
    #print(all_time_connected_machines)
    total_connection_periods=total_connection_periods.filter(lambda machine:machine[1]==0).union(all_time_connected_machines) #(machine_ID, connection period in the cluster)
    
    cluster_total_cpu=machine_events.filter(lambda m: m[4]!='').map(lambda m: (m[1],float(m[4]))).reduceByKey(max)
    cluster_total_cpu_power=total_connection_periods.join(cluster_total_cpu).map(lambda m:m[1][0]*m[1][1]).collect()
    
    print('Total CPU loss percentage on the Google cluster due to maintenance is:',sum(cpu_losses)/sum(cluster_total_cpu_power))

    # Calculate total memory power loss for all machines and then sum to get the total memory power loss on the cluster
    memory_capacities= machine_events.filter(lambda m: int(m[1]) in rec_IDs and m[4]!='' and m[5]!='').map(lambda m: (m[1],float(m[5]))).reduceByKey(max)
    memory_losses=disconnection_time.join(memory_capacities).map(lambda c: sum([c[1][1]*x for x in c[1][0]])).collect()
    #print('Memory losses on different machines due to maintenance are:',memory_losses)

    # We do the same as cpu resource
    cluster_total_memory=machine_events.filter(lambda m: m[5]!='').map(lambda m: (m[1],float(m[5]))).reduceByKey(max)
    cluster_total_memory_power=total_connection_periods.join(cluster_total_memory).map(lambda m:m[1][0]*m[1][1]).collect()

    print('Total memory loss percentage on the Google cluster due to maintenance is:',sum(memory_losses)/sum(cluster_total_memory_power))
    
    X_axis = np.arange(99)

    plt.bar(X_axis - 0.2, cpu_losses[:99], 0.4, label = 'CPU losses')
    plt.bar(X_axis + 0.2, memory_losses[:99], 0.4, label = 'Memory losses')
    
    plt.xticks(X_axis, X_axis)
    plt.xlabel("Maintained machines")
    plt.ylabel("Power loss")
    plt.title("Computational power losses for maintained machines")
    plt.legend()
    plt.show()
    return


if __name__=="__main__":
	main()