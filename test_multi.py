# Libraries
import time
import json
import os
from datetime import datetime, timedelta
import pendulum

# Airflow
from airflow.models import DAG, Variable
from airflow.decorators import task


LOCAL_TZ = pendulum.timezone("Asia/Bangkok")

DAG_NAME = "stacktonic_example_dag" # DAG name (proposed format: lowercase underscore). Should be unique.
DAG_DESCRIPTION = "Example DAG by Chinnapat"
DAG_START_DATE = datetime(2022, 3, 15, tzinfo=LOCAL_TZ) # Startdate. When setting the "catchup" parameter to True, you can perform a backfill when you insert a specific date here like datetime(2021, 6, 20)
DAG_SCHEDULE_INTERVAL = "@daily" # Cron notation -> see https://airflow.apache.org/scheduler.html#dag-runs
DAG_CATCHUP = False # When set to true, DAG will start running from DAG_START_DATE instead of current date
DAG_PAUSED_UPON_CREATION = True # Defaults to False. When set to True, uploading a DAG for the first time, the DAG doesn't start directly 
DAG_MAX_ACTIVE_RUNS = 5 # Configure efficiency: Max. number of active runs for this DAG. Scheduler will not create new active DAG runs once this limit is hit. 

default_args = {
    "owner": "airflow",
    "start_date": DAG_START_DATE,
    "depends_on_past": False,
    "email": Variable.get("email_monitoring", default_var="<FALLBACK-EMAIL>"), # Make sure you create the "email_monitoring" variable in the Airflow interface
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2, # Max. number of retries before failing
    "retry_delay": timedelta(minutes=60) # Retry delay
}

with DAG(
    DAG_NAME,
    description=DAG_DESCRIPTION,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    is_paused_upon_creation=DAG_PAUSED_UPON_CREATION,
    default_args=default_args) as dag:

    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    run_this = print_context()

    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):

        @task(task_id=f'sleep_for_{i}')
        def my_sleeping_function(random_base):
            """This is a function that will run within the DAG execution"""
            time.sleep(random_base)

        sleeping_task = my_sleeping_function(random_base=float(i) / 10)

        run_this >> sleeping_task
