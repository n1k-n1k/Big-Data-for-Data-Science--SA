## Проект часть III

### ML Engineering

Мы с вами уже обработали данные, настало время использовать их для создания моделей.

Мы выделили ключевые параметры и определили целевой признак (`CTR`), который мы бы хотели предсказывать по параметрам объявления. 
При этом мы хотим строить модели периодически по новым данным и использовать ее над входящими данными, поэтому нам понадобятся две `Spark`-задачи для выполнения.

Структура данных, которую мы будем использовать (результат предыдущего шага):

- `ad_id` [integer] - id рекламного объявления
- `target_audience_count` [decimal] -	размер аудитории, на которую таргетируется объявление
- `has_video` [integer] - 1 если есть видео, иначе 0
- `is_cpm` [integer] - 1 если тип объявления CPM, иначе 0
- `is_cpc` [integer] - 1 если тип объявления CPC, иначе 0
- `ad_cost` [double] - стоимость объявления в рублях
- `day_count` [integer] - Число дней, которое показывалась реклама
- `ctr`	[double] - Отношение числа кликов к числу просмотров

Представьте себя в роли инженера по машинному обучению.

Теперь по отобранным данным после их обработки, мы хотели бы создать модель для предсказания `CTR`.

Ваша команда ученых по данным работала с маленькой выборкой данных в 4Гб от общего датасета данных 
и рекомендует применять линейную регрессию со следующими параметрами:
```
maxIter=40, regParam=0.4, elasticNetParam=0.8
```
Однако вам необходимо реализовать распределенную модель, выбрать тип из возможных, для этого, вы можете задействовать 
любую модель регрессии (DecisionTreeRegressor, RandomForestRegressor, GBTRegressor), 
подобрать оптимальные гиперпараметры и сравнить результаты.

Как результат реализуйте две `PySpark` задачи и загрузите их в ответ (можно добавить Jupyter Notebook с анализом моделей):

1) `PySparkMLFit.py` - задача, которая должна тренировать модель, подбирать оптимальные гиперпараметры на входящих данных, 
сохранять ее и производить оценку качества модели, используя `RegressionEvaluator` и выводя в консоль `RMSE` модели на основе test датасета.
Варианты запуска задачи:
```
spark-submit PySparkMLFit.py train.parquet test.parquet
#или
python PySparkMLFit.py train.parquet test.parquet
```
где:
`train.parquet` - путь к датасету, который необходимо использовать для обучения
`test.parquet` - путь к датасету, который необходимо использовать для оценки полученной модели

2) `PySparkMLPredict.py` - задача, которая должна загружать модель и строить предсказание над переданными ей данными.
Варианты запуска задачи:

```
spark-submit PySparkMLPredict.py test.parquet result
#или
python PySparkMLPredict.py test.parquet result
```
где:
`test.parquet` - путь к датасету, на основе данных которого нужно выполнить предсказания CTR
`result` - путь, по которому будет сохранен результат предсказаний в формате CSV следующего вида `[ad_id, prediction]`

Пожалуйста, используйте [Pipeline](https://spark.apache.org/docs/latest/ml-pipeline.html#example-pipeline).

* [Документация](https://spark.apache.org/docs/latest/ml-pipeline.html)
* Пожалуйста, используйте шаблоны [PySparkMLFit.py](https://raw.githubusercontent.com/AlexKbit/stepik-ds-course/master/Week5/SparkML/Project/PySparkMLFit.py), 
[PySparkMLPredict.py](https://raw.githubusercontent.com/AlexKbit/stepik-ds-course/master/Week5/SparkML/Project/PySparkMLPredict.py)
* Файлы с данными для отладки: [train.parquet](https://github.com/AlexKbit/stepik-ds-course/raw/master/Week5/SparkML/Project/train.parquet), 
[test.parquet](https://github.com/AlexKbit/stepik-ds-course/raw/master/Week5/SparkML/Project/test.parquet)

**Формат решения**: два `Python` файла с реализованным кодом (обязательно). `Jupyter Notebook` (опционально)

##### Рецензия преподавателя:
```
Отличное решение, вы разобрались с Pipeline моделью. 
Важно понимать, что модель основанная на Pipeline, сохраняет все наборы трансформации, которые необходимо применять к данным, 
непосредственно переде передачей данных в модель, это позволяет легко использовать ее без предварительной обработки данных. 
Вы так же могли сразу собирать паиплаины и передавать в TVS, он всеравно будет обучать именно модель и результатом будет уже PipelineModel.  
```
