"""
4.5.7.
Создайте DAG c идентификатором: trigger_dag в переменной dag, установите дату начала выполнения как 01-12-2019,
а в расписании '@once'.
Создайте и назначьте в DAG два TriggerDagRunOperator с task_id: trigger_job1, trigger_job2, так что:
trigger_job1 - вызывает job1_dag DAG.
trigger_job2 - вызывает job2_dag DAG.
И обе эти задачи выполняются параллельно.
"""

from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator


def job1_dag():
    pass


def job2_dag():
    pass


dag = DAG(
    dag_id='trigger_dag',
    start_date=datetime(2019, 12, 1),
    schedule_interval='@once')

trigger_job1 = TriggerDagRunOperator(
    task_id='trigger_job1',
    trigger_dag_id='job1_dag',
    python_callable=job1_dag,
    dag=dag)

trigger_job2 = TriggerDagRunOperator(
    task_id='trigger_job2',
    trigger_dag_id='job2_dag',
    python_callable=job2_dag,
    dag=dag)
