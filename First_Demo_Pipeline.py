#Step-1: Importing all the modules required to build to a pipeline

import datetime as dt
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Step-2: Setting up default arguments
default_args = {
    'owner' : 'nivethan',
    # By specifying some date in the past, airflow will start from that day, in this case 3 days ago.
    'start_date' : airflow.utils.dates.days_ago(3),
    # If we dont specify the end date, it will execute daily at that particular time in future from the start time and date
    # In this case, by specifying end_date, pipeline will run and stop at that particular date as we mentioned
    'end_date' : dt(2020, 9, 25),
    # Setting "depends_on_past" as False will make the task run, even if it is failed on previous day,
    # If it set true, if the task fails previous day, the current day task will not run.
    'depends_on_past' : False,
    # Setting up email through which we want to recieve notifications from
    'email' : ['nivemeche@gmail.com'],
    # By setting True, we will receive notification through email, from the airflow if any failures happened.
    'email_on_failure' : False,
    # By setting True, will receive email if every time the retry happens.
    'email_on_retry' : False,
    # If task fails, the number of retries that we want to have after failure happens.
    # By setting retry_delay time, the amount of time it takes to retry after the failure has happened.
    'retries' : 1,
    'retry_delay' : timedelta(minutes= 5)
}

# Step-3: Instantiating a DAG,
# Giving DAG Name, configure the schedule, and set the DAG settings
dag = DAG('First_Demo',
          default_args = default_args,
          description = 'Initial Demo Pipeline DAG',
          # How often a DAG should be triggered which can hold a specific time to trigger,
          # and date and time it should have to start.
          schedule_interval = timedelta(days=1),)

# Step-4: Tasks: Layout all the tasks in the workflow
task1 = BashOperator(
    task_id = 'print_date',
    bash_command = 'date',
    dag = dag,
)

task2 = BashOperator(
    task_id = 'sleep',
    depends_on_past = False,
    bash_command = 'sleep 5',
    dag = dag,
)

templated_commamd = """
{%for i in range(5)%}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{%endfor%}"""

task3 = BashOperator(
    task_id = 'templated',
    depends_on_past = False,
    bash_command = templated_commamd,
    params = {'my_param' : 'Parameter I passed in'},
    dag = dag,
)

task1 >> [task2, task3]