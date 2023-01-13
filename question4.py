import os
from pyspark import SparkContext
import matplotlib.pyplot as plt

def main():
    sc = SparkContext("local[2]")
    sc.setLogLevel("ERROR")
    
    # 4. Do tasks with a low scheduling class have a higher probability of being evicted?
    # read the input files into an RDD[String], 3 files of Task_events table were used. 
    task_events = sc.textFile("./Task_events/*")

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

if __name__=="__main__":
	main()