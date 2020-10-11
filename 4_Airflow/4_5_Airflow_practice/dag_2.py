"""
4.5.2.
Создайте DAG c идентификатором: dummy_dag в переменной dag.
Дату начала выполнения start_date укажите как dag_start_date.
Создайте и назначьте в DAG три DummyOperator с task_id: dummy_operator1, dummy_operator2, dummy_operator3.
И определите следующий порядок выполнения:
dummy_operator1 -> dummy_operator2 -> dummy_operator3
"""

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='dummy_dag',
    start_date=dag_start_date,
    schedule_interval='@once')

dummy_operator1 = DummyOperator(task_id='dummy_operator1', dag=dag)
dummy_operator2 = DummyOperator(task_id='dummy_operator2', dag=dag)
dummy_operator3 = DummyOperator(task_id='dummy_operator3', dag=dag)

dummy_operator1 >> dummy_operator2 >> dummy_operator3
