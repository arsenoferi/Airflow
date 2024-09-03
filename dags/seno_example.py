from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2023, 1, 1),
    schedule ='@daily',
    catchup=False,
    tags=['seno_example'],    
)

def seno_example():
    
    @task
    def task1():
        print('task1')
        return 1
    
    @task
    def task2(value):
        print('task2')
        return 2 + value
    
    task2(task1())

seno_example()