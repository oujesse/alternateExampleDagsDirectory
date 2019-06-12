from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# The default arguments for this file's DAG
default_args = {
    'owner': 'dag1owner',
    'start_date': datetime(2014, 6, 3),
    'depends_on_past': False,
    'email': ['dags2@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

schedule_interval = timedelta(seconds=40)

# DAGS/tasks in Airflow are automatically constructed through global variables
alternate_dag2 = DAG("alternate_dag2", default_args=default_args, schedule_interval=schedule_interval)

# Functions to be converted into tasks
def printA():
    print("a")

printATask = PythonOperator(
    task_id="printATask",
    python_callable=printA,
    dag=alternate_dag2
)

def addThenPrint():
    a = 45
    b = 33
    print(a + b)

addThenPrintTask = PythonOperator(
    task_id="addThenPrintTask",
    python_callable=addThenPrint,
    dag=alternate_dag2
)

# Dependencies
printA >> addThenPrint