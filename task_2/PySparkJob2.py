import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

ROOT_DIR = Path(os.getcwd()).parent.absolute()


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.
    Найдите топ 10 авиамаршрутов (ORIGIN_AIRPORT, DESTINATION_AIRPORT)
    по наибольшему числу рейсов, а так же посчитайте среднее время в полете (AIR_TIME)

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """

    flights_df = spark.read.parquet(flights_path)
    res_df = flights_df[['ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'AIR_TIME', 'TAIL_NUMBER']] \
        .groupby(['ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']) \
        .agg(F.count('TAIL_NUMBER').alias('tail_count'), F.avg('AIR_TIME').alias('avg_air_time')) \
        .orderBy(F.col('tail_count').desc()) \
        .limit(10)
    res_df.show(truncate=False, n=100)
    res_df.write.mode("overwrite").parquet(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    # print(ROOT_DIR)
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path',
                        type=str,
                        default=str(ROOT_DIR) + '/flights.parquet',
                        help='Please set flights datasets path.')
    parser.add_argument('--result_path',
                        type=str,
                        default=str(ROOT_DIR) + '/task_1/result',
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)