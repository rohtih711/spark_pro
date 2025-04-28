from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta


def hello_world():
    print("Hello World from Python!")

default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dummy_dag',
    default_args=default_args,
    description='A simple dummy DAG',
    schedule_interval=None,
    catchup=False,
    tags=['dev']
)

t1 = BashOperator(
    task_id ='besh_hello',
    bash_command='echo "Hello World"',
    dag=dag,
)

t2 = PythonOperator(
    task_id='python_hello',
    python_callable= hello_world,
    dag=dag,
)

t1 >> t2