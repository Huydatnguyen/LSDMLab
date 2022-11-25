
class Machine_events_table:
      TIME_STAMP=0
      MACHINE_ID=1
      EVENT_TYPE=2
      PLATFORM_ID=3
      CPU_CAPACITY=4
      MEM_CAPACITY=5


class Job_events_table:
      TIME_STAMP=0
      JOB_ID=2
      EVENT_TYPE=3
      USER_NAME=4
      SCHEDULING_CLASS=5
      JOB_NAME=6
      LOGICAL_JOB_NAME=7

class Task_events_table:
      TIME_STAMP=0
      JOB_ID=2
      TASK_INDEX=3
      MACHINE_ID=4 
      EVENT_TYPE=5
      USER_NAME=6
      SCHEDULING_CLASS=7
      PRIORITY=8
      CPU_REQUEST=9
      MEMORY_REQUEST=10
      DISK_REQUEST=11

