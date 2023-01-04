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
    cpu_capacities= machine_events.filter(lambda m: int(m[1]) in rec_IDs and m[4]!='' and m[5]!='').map(lambda m: (m[1],float(m[4]))).groupByKey().map(lambda m: (m[0],max(list(m[1]))))
    cpu_losses=disconnection_time.join(cpu_capacities).map(lambda c: sum([c[1][1]*x for x in c[1][0]])).collect()
    #print('CPU losses on different machines due to maintenance are:',cpu_losses)
    print('Total CPU loss on the Google cluster due to maintenance is:',sum(cpu_losses))

    # Calculate total memory power loss for all machines and then sum to get the total memory power loss on the cluster
    memory_capacities= machine_events.filter(lambda m: int(m[1]) in rec_IDs and m[4]!='' and m[5]!='').map(lambda m: (m[1],float(m[5]))).groupByKey().map(lambda m: (m[0],max(list(m[1]))))
    memory_losses=disconnection_time.join(memory_capacities).map(lambda c: sum([c[1][1]*x for x in c[1][0]])).collect()
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
    return


if __name__=="__main__":
	main()