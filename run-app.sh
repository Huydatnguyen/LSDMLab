
# activate your conda environment to run the spark application
eval "$(conda shell.bash hook)"

conda activate lsdm

echo "*********************** Analyzing Google cluster traces with Spark ***********************"
while true
do
echo "
1.  What is the distribution of the machines according to their CPU capacity?

2.  What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?

3.  What is the distribution of the number of jobs/tasks per scheduling class?

4.  Do tasks with a low scheduling class have a higher probability of being evicted?

5.  In general, do tasks from the same job run on the same machine?

6.  Are the tasks that request the more resources the one that consume the more resources?

7.  Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?

8.  Do tasks having the higher priority require more resources?

9.  What are hardware specifications of machines on which different priority tasks have/haven't successfully run?
"

echo "Please enter the number of your question, or press ENTER to quit: "

read input

    case $input in
        "1")
            python question1-solution3.py
        ;;
        "2")
            python question2.py
        ;;
        "3")
            python question3.py
        ;;
        "4")
            python question4.py
        ;;
        "5")
            python question5.py
        ;;
        "6")
            python question6.py
        ;;
        "7")
            python question7.py
        ;;
        "8")
            python question8.py
        ;;
        "9")
            python question9.py
        ;;
        "")
            exit
        ;;
    esac
done