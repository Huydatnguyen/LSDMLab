# LSDMLab
Process large amount of data and to implement complex data analyses using Spark. The dataset has been made available by Google. It includes data about a cluster of 12500 machines, and the activity on this cluster during 29 days.

To start the Spark application, please run the script run-app.sh.

## 1. Lab tasks
The following questions are answered in this lab:

• What is the distribution of the machines according to their CPU capacity?

• What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?

• What is the distribution of the number of jobs/tasks per scheduling class?

• Do tasks with a low scheduling class have a higher probability of being evicted?

• In general, do tasks from the same job run on the same machine?

• Are the tasks that request the more resources the one that consume the more resources?

• Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?

• Do tasks having the higher priority require more resources?

• What are hardware specifications of machines on which different priority tasks have/haven't successfully run?
## 2. Extending the work
### 2.1 Comparison of different solutions
The solution is compared from different points of view (performance, ease of use,etc.) with different technical solutions to process the data:

• Compare with Spark Dataframe.

• Compare the use of Spark to the use of a non-parallel Python data analysis library such us Pandas.
### 2.2 Deploying in the cloud
Our Spark application is deployed in a real distributed environment (GCP Dataproc cluster) to conduct some performance evaluation in this context.
## 3. Instructions for running the code
- Ensures Spark is installed and configured properly.
- Ensures the working directory contains well-named folders (Job_events, Machine_events, Task_events, Task_usage) including the number of input files (only .csv files accepted).
- run script run-app.sh and enter the question to start the execution.

