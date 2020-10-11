"""
4.5.1.
Создайте DAG c идентификатором: dummy_dag в переменной dag.
Дату начала выполнения start_date укажите как dag_start_date.
Создайте и назначьте в DAG один DummyOperator с task_id: dummy_operator1.
"""

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='dummy_dag',
    start_date=dag_start_date,
    schedule_interval='@once')

dummy_operator1 = DummyOperator(task_id='dummy_operator1', dag=dag)
