## [Stepik Academy] Big Data для Data Science
https://academy.stepik.org/big-data

`Click House` `AirFlow` `PySpark` `SparkML` `Hadoop`

### [Проект:](_Project)


#### I - [Data Analysis & ClickHouse](_Project/1_DA_ClickHouse/)
Исследование данных. 

#### II - [Data Engineering](_Project/2_Data_Engineering)
Обработка данных посредством `PySpark` (`parquet`-файлы) и подготовка их к обучению моделей в виде `PySpark`-задачи (`PySparkJob.py`).

####  III - [ML Engineering](_Project/3_ML_Engineering)
Cоздание паспределённой модели в `PySparkML` для предсказания `CTR` в виде двух `PySpark` задач:
1. `PySparkMLFit.py` - задача, которая должна тренировать модель, подбирать оптимальные гиперпараметры на входящих данных, 
сохранять ее и производить оценку качества модели, используя RegressionEvaluator и выводя в консоль RMSE модели на основе test датасета.
2. `PySparkMLPredict.py` - задача, которая должна загружать модель и строить предсказание над переданными ей данными.

### Содержание курса

1. **Введение в Big Data для Data Science**
    - Зачем DS знать Big Data?
    - Хранение данных
    - Обработка данных
    - SQL и ClickHouse

2. **Hadoop**
    - Архитектура Hadoop
    - HDFS
    - MapReduce
    - Hive
    - Hbase

3. **Spark**
    - Архитектура spark
    - Spark Core
    - Потоковая обработка данных

4. **Workflow**
    - Управление данными ETL/ELT
    - Архитектура хранилищ Data Warehouse vs Data Lake
    - Облачные решения
    - Apache Airflow

5. **SparkML**
    - Распределенные модели машинного обучения
    - Spark ML компоненты и модели
    - Поставка моделей

6. **BI Tools**
    - Работа с данными в Superset
    - Аналитические агрегаты

7. **Проект**

    В проекте вы будете представлять себя рекламным аналитиком: сначала вы должны исследовать и выкачать данные (ClickHouse),
потом обработать их и обучить модель с этими данными (PySpark), а в конце визуализировать выводы и создать отчеты (Superset).

<a href="https://stepik.org/cert/797811">[Сертификат об окончании курса]</a>
