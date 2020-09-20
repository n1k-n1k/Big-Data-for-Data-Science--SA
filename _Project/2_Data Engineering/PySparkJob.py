import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# config
TRAIN_SIZE = 0.5
TEST_SIZE = 0.25
VALIDATE_SIZE = 0.25

TRAIN_SUBPATH = 'train'
TEST_SUBPATH = 'test'
VALIDATE_SUBPATH = 'valid'


def process(spark, input_file, target_path):
    spark.conf.set('spark.sql.session.timeZone', 'GMT+3')

    df = spark.read.parquet(input_file)
    result_df = get_features(df)
    train_df, test_df, validate_df = split(result_df,
                                           train_size=TRAIN_SIZE,
                                           test_size=TEST_SIZE,
                                           valid_size=VALIDATE_SIZE)

    write_data(train_df, os.path.join(target_path, TRAIN_SUBPATH), partitions=1)
    write_data(test_df, os.path.join(target_path, TEST_SUBPATH), partitions=1)
    write_data(validate_df, os.path.join(target_path, VALIDATE_SUBPATH), partitions=1)


def split(df, train_size, test_size, valid_size):
    return df.randomSplit([train_size, test_size, valid_size])


def write_data(df, path_, partitions=1):
    df.coalesce(partitions).write.parquet(path_)


def get_features(df):
    ndf = df.withColumn('is_cpm', F.when(col('ad_cost_type') == 'CPM', 1).otherwise(0)) \
        .withColumn('is_cpc', F.when(col('ad_cost_type') == 'CPC', 1).otherwise(0)) \
        .withColumn('is_view', F.when(col('event') == 'view', 1).otherwise(0)) \
        .withColumn('is_click', F.when(col('event') == 'click', 1).otherwise(0))

    result_df = ndf.groupBy('ad_id') \
        .agg(F.max(col('target_audience_count')),
             F.max(col('has_video')),
             F.max(col('is_cpm')),
             F.max(col('is_cpc')),
             F.max(col('ad_cost')),
             F.sum(col('is_view')),
             F.sum(col('is_click')),
             F.countDistinct(col('date')).astype('int')) \
        .withColumnRenamed('max(target_audience_count)', 'target_audience_count') \
        .withColumnRenamed('max(has_video)', 'has_video') \
        .withColumnRenamed('max(is_cpm)', 'is_cpm') \
        .withColumnRenamed('max(is_cpc)', 'is_cpc') \
        .withColumnRenamed('max(ad_cost)', 'ad_cost') \
        .withColumnRenamed('sum(is_view)', 'views_cnt') \
        .withColumnRenamed('sum(is_click)', 'cliks_cnt') \
        .withColumnRenamed('CAST(count(DISTINCT date) AS INT)', 'day_count') \
        .withColumn('CTR', col('cliks_cnt') / col('views_cnt')) \
        .drop(col('views_cnt')) \
        .drop(col('cliks_cnt')) \
        .sort('ad_id', ascending=True)

    return result_df


def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    target_path = argv[1]
    print("Target path: " + target_path)
    spark = _spark_session()
    process(spark, input_path, target_path)
    sys.exit('Done')


def _spark_session():
    return SparkSession.builder.appName('PySparkJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit('Input and Target path are required')
    elif not os.path.exists(arg[0]):
        sys.exit(f'Input data "{arg[0]}" is not exist, run with correct Input data')
    elif os.path.exists(arg[1]):
        sys.exit(f'Target path "{arg[1]}" already exists, run with another Target path')
    else:
        main(arg)
