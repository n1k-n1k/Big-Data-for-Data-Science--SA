"""
4.5.3.
Создайте DAG c идентификатором: task_dag в переменной dag.
Дату начала выполнения start_date укажите как dag_start_date.
Создайте и назначьте в DAG пять DummyOperator с task_id: task1, task2, task3, task4, task5.
И определите следующий порядок выполнения:
         -> task4 -->
       /               \
task1 -> task2 -> task3 -> task5

"""

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='task_dag',
    start_date=dag_start_date,
    schedule_interval='@once')

task1 = DummyOperator(task_id='task1', dag=dag)
task2 = DummyOperator(task_id='task2', dag=dag)
task3 = DummyOperator(task_id='task3', dag=dag)
task4 = DummyOperator(task_id='task4', dag=dag)
task5 = DummyOperator(task_id='task5', dag=dag)

task1 >> task4 >> task5
task1 >> task2 >> task3 >> task5
