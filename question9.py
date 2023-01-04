import os
from pyspark import SparkContext

def main():
    sc = SparkContext("local[*]")
    sc.setLogLevel("ERROR")
    printed=False
    # 9. What are hardware specifications of machines on which different priority tasks have/haven't successfully run?
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

    # create machine attributes RDD
    machine_attributes = sc.parallelize([])
    files=os.listdir("machine_attributes/")
    for file in files:
        machine_att_rdd = sc.textFile("machine_attributes/"+file)
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
        hardware_finished=machine_attributes.join(machine_id_finished_tasks).map(lambda m: (m[0],(m[1][0][0],m[1][0][1]))).collect() #(machine ID, (machine attribute, attribute value))
        hardware_failed=machine_attributes.join(machine_id_failed_tasks).map(lambda m: (m[0],(m[1][0][0],m[1][0][1]))).collect() #(machine ID, (machine attribute, attribute value))
        # since the logs are too large, we print hardware specifications only for priority 0 to show the result
        if not printed:
            print('Hardware characteristics for machines that have successfully run tasks with priority {} are: {}'.format(priority,hardware_finished))
            print('Hardware characteristics for machines that have failed to run tasks with priority {} are: {}'.format(priority,hardware_failed))
            printed=True

if __name__=="__main__":
	main()