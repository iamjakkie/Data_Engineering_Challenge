from datetime import timedelta
import airflow 
from airflow import DAG 
from airflow.operators.dummy import DummyOperator

args = {
    'owner': 'jakkie',
    'start_date': datetime(2021, 5, 14)
}

dag = DAG(
    'task5-dag',
    default_args=args,
    description='dag for task 5'
)
with dag:
    task1 = DummyOperator(task_id='Task_1')
    task2 = DummyOperator(task_id='Task_2')
    task3 = DummyOperator(task_id='Task_3')
    task4 = DummyOperator(task_id='Task_4')
    task5 = DummyOperator(task_id='Task_5')
    task6 = DummyOperator(task_id='Task_6')

    task1.set_downstream([task2, task3])
    task2.set_downstream([task4, task5, task6])
    task3.set_downstream([task4, task5, task6])


