import sys
import pandas as pd
import time

# Question 1 Pandas____________________________________________________________start

# start timer
start = time.time()

# read all the input files into an Dataframe in pandas
df = pd.read_csv('./Machine_events/part-00000-of-00001.csv')

# rename columns in order to access easily
df.columns = ["timeStamp","machineID","eventType","platformID","cpu","memory"]

# collect the identical data about cpu capacity into groups and count them 
df = df.groupby(['cpu']).count()

# show the result
print(df) 

# end timer
end = time.time()
print("elapsed time:  " , end-start)

# Question 1 Pandas______________________________________________________________end

input("Press Enter to continnnue...")
