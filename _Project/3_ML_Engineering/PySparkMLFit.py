import io
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession

MODEL_PATH = r'_models\spark_ml_model'
LR_PARAMS_BASE = {
    'maxIter': 40,
    'regParam': 0.4,
    'elasticNetParam': 0.8}


def process(spark, train_data, test_data):
    df_train = spark.read.parquet(train_data)
    df_test = spark.read.parquet(test_data)

    features = VectorAssembler(inputCols=df_train.columns[1:-1], outputCol='features')
    evaluator = RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse')
    lr_model_base = LinearRegression(labelCol='ctr', **LR_PARAMS_BASE)
    lr_model_to_tune = LinearRegression(labelCol='ctr')

    lr_param_grid = ParamGridBuilder() \
        .addGrid(lr_model_to_tune.maxIter, [5, 10, 20, 40, 50]) \
        .addGrid(lr_model_to_tune.regParam, [0.4, 0.1, 0.01, 0.001]) \
        .addGrid(lr_model_to_tune.fitIntercept, [False, True]) \
        .addGrid(lr_model_to_tune.elasticNetParam, [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]) \
        .build()

    tvs = TrainValidationSplit(estimator=lr_model_to_tune,
                               estimatorParamMaps=lr_param_grid,
                               evaluator=evaluator,
                               trainRatio=0.8)

    pipeline_model_base = Pipeline(stages=[features, lr_model_base]).fit(df_train)
    prediction_base = pipeline_model_base.transform(df_test)
    rmse_base = evaluator.evaluate(prediction_base)
    print(f'Base lr model params: {LR_PARAMS_BASE}')
    print(f'RMSE at base lr model = {rmse_base}')

    print('Tuning lr model...')
    pipeline_model_tuned = Pipeline(stages=[features, tvs]).fit(df_train)
    prediction_tuned = pipeline_model_tuned.transform(df_test)
    rmse_tuned = evaluator.evaluate(prediction_tuned)

    model_java_obj = pipeline_model_tuned.stages[-1].bestModel._java_obj
    lr_params_tuned = {
        'maxIter': model_java_obj.getMaxIter(),
        'regParam': model_java_obj.getRegParam(),
        'elasticNetParam': model_java_obj.getElasticNetParam(),
        'fitIntercept': model_java_obj.getFitIntercept()}

    print(f'Base lr model params: {lr_params_tuned}')
    print(f'RMSE at tuned lr model = {rmse_tuned}')

    if rmse_tuned < rmse_base:
        pipeline_model_tuned.write().overwrite().save(MODEL_PATH)
        print(f'Tuned model has better RMSE value')
    else:
        pipeline_model_base.write().overwrite().save(MODEL_PATH)
        print(f'Base model has better RMSE value')
    print(f'Model saved at "{MODEL_PATH}"')

    spark.stop()


def main(argv):
    train_data = argv[0]
    print("Input path to train data: " + train_data)
    test_data = argv[1]
    print("Input path to test data: " + test_data)
    spark = _spark_session()
    process(spark, train_data, test_data)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Train and test data are require.")
    else:
        main(arg)
