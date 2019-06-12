from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# The default arguments for this file's DAG
default_args = {
    'owner': 'dag1owner',
    'start_date': datetime(2016, 6, 10),
    'depends_on_past': True,
    'email': ['dags1@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

schedule_interval = timedelta(seconds=30)

# DAGS/tasks in Airflow are automatically constructed through global variables
alternate_dag1 = DAG("alternate_dag1", default_args=default_args, schedule_interval=schedule_interval)

# Functions to be converted into tasks
def printHello():
    print("hello")

printHelloTask = PythonOperator(
    task_id="printHelloTask",
    python_callable=printHello,
    dag=alternate_dag1
)

def printBye():
    print("bye")

printByeTask = PythonOperator(
    task_id="printByeTask",
    python_callable=printBye,
    dag=alternate_dag1
)

def printGreetings():
    print("greetings")

printGreetingsTask = PythonOperator(
    task_id="printGreetingsTask",
    python_callable=printGreetings,
    dag=alternate_dag1
)

# Dependencies
printHelloTask >> printByeTask << printGreetingsTask

