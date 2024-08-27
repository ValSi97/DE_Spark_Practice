import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

ROOT_DIR = Path(os.getcwd()).parent.absolute()


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.
    Аналитик попросил определить список аэропортов у которых самые больше проблемы с задержкой на вылет рейса.
    Для этого необходимо вычислить среднее, минимальное, максимальное время задержки
    и выбрать аэропорты только те где максимальная задержка (DEPARTURE_DELAY) 1000 секунд и больше.
    Дополнительно посчитать корреляцию между временем задержки и днем недели (DAY_OF_WEEK)

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    flights_df = spark.read.parquet(flights_path)
    res_df = flights_df[['ORIGIN_AIRPORT', 'DEPARTURE_DELAY', 'DAY_OF_WEEK']] \
        .groupby(['ORIGIN_AIRPORT']) \
        .agg(F.avg('DEPARTURE_DELAY').alias('avg_delay'),
             F.min('DEPARTURE_DELAY').alias('min_delay'),
             F.max('DEPARTURE_DELAY').alias('max_delay'),
             F.corr('DEPARTURE_DELAY', 'DAY_OF_WEEK').alias('corr_delay2day_of_week')) \
        .filter(F.col('max_delay') > 1000)
        #.orderBy(F.col('avg_delay').desc()) \

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
    return SparkSession.builder.appName('PySparkJob3').getOrCreate()


if __name__ == "__main__":
    # print(ROOT_DIR)
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path',
                        type=str,
                        default=str(ROOT_DIR) + '/flights.parquet',
                        help='Please set flights datasets path.')
    parser.add_argument('--result_path',
                        type=str,
                        default=str(ROOT_DIR) + '/task_3/result',
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
