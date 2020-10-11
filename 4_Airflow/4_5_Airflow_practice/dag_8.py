"""
4.5.8.
Создайте DAG c идентификатором: spark_submit_dag в переменной dag.
Дату начала выполнения start_date укажите как 1-12-2019 и ежедневное выполнение по расписанию '@daily'.
Создайте и назначьте в DAG один SparkSubmitOperator с task_id: spark_submit,
который будет запускать Spark приложение из файла PySparkJob.py с аргументами input.csv и output.csv,
используя соединение 'spark_default'.
"""

from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

dag = DAG(
    dag_id='spark_submit_dag',
    start_date=datetime(2019, 12, 1),
    schedule_interval='@daily')

submit_operator = SparkSubmitOperator(
    task_id='spark_submit',
    application='PySparkJob.py',
    application_args=['input.csv', 'output.csv'],
    conn_id='spark_default',
    dag=dag)
