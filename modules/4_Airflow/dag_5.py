"""
4.5.5.
Создайте DAG c идентификатором: prepare_dag в переменной dag, установите дату начала выполнения как 21-11-2019.
Создайте и назначьте в DAG три BashOperator с task_id: init, prepare_train, prepare_test.
init - вызывает bash-скрипт /usr/bin/init.sh
prepare_train - вызывает bash-скрипт /usr/bin/prepare_train.sh
prepare_test - вызывает bash-скрипт /usr/bin/prepare_test.sh
Порядок выполнения:
          -> prepare_train
       /
init -
       \
         - > prepare_test
"""

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id='prepare_dag',
    start_date=datetime(2019, 11, 21),
    schedule_interval='@once')

init = BashOperator(
    task_id='init',
    bash_command='/usr/bin/init.sh ',
    dag=dag)

prepare_train = BashOperator(
    task_id='prepare_train',
    bash_command='/usr/bin/prepare_train.sh ',
    dag=dag)

prepare_test = BashOperator(
    task_id='prepare_test',
    bash_command='/usr/bin/prepare_test.sh ',
    dag=dag)

init >> [prepare_train, prepare_test]
