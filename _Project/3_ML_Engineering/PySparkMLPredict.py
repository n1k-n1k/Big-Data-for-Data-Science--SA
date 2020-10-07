import sys

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

MODEL_PATH = r'_models\spark_ml_model'


def process(spark, input_file, output_file, partitions=1):
    df = spark.read.parquet(input_file)
    loaded_model = PipelineModel.load(MODEL_PATH)
    prediction = loaded_model.transform(df)
    prediction.select('ad_id', 'prediction').coalesce(partitions).write.parquet(output_file)
    spark.stop()


def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    output_file = argv[1]
    print("Output path to file: " + output_file)
    spark = _spark_session()
    process(spark, input_path, output_file)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLPredict').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)
