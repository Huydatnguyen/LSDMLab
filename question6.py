import os
from pyspark import SparkContext

def main():
    sc = SparkContext("local[2]")
    sc.setLogLevel("ERROR")

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
    tasks_big=tasks_big.map(lambda t: (t[2]+t[3],1)).distinct()
    # print('requests: ',tasks_big)

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

    filtered_2=resource_usage.filter(lambda t: t[5]!='' and t[6]!='' and t[12]!='')
    # extract tasks that consume big resources
    rsc_big=filtered_2.filter(lambda t: float(t[5])>0.5 or float(t[6])>0.5 or float(t[12])>0.5)
    rsc_big=rsc_big.map(lambda t: (t[2]+t[3],1)).distinct()
    # print('usage: ',rsc_big)

    nb_tasks_big_rsc=rsc_big.join(tasks_big).count()
    print("Percentage of tasks consuming big resources among those requesting big resources: ",nb_tasks_big_rsc/rsc_big.count())

if __name__=="__main__":
	main()