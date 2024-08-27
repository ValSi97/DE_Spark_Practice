import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

ROOT_DIR = Path(os.getcwd()).parent.absolute()

def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    flights_df = spark.read.parquet(flights_path)[['DEPARTURE_DELAY',
                                                   'CANCELLED',
                                                   'DIVERTED',
                                                   'DISTANCE',
                                                   'AIRLINE',
                                                   'AIR_TIME',
                                                   'CANCELLATION_REASON']]
    airlines_df = spark.read.parquet(airlines_path)[['IATA_CODE', 'AIRLINE']] \
        .withColumnRenamed('AIRLINE', 'AIRLINE_NAME')
    res_df = airlines_df.join(other=flights_df, on=flights_df['AIRLINE'] == F.col('IATA_CODE'), how='inner') \
        .filter(F.col('AIRLINE_NAME').isNotNull()) \
        .groupby('AIRLINE_NAME') \
        .agg((F.count('AIRLINE') - F.sum('DIVERTED') - F.sum('CANCELLED')).alias('correct_count'),
             F.sum('DIVERTED').alias('diverted_count'),
             F.sum('CANCELLED').alias('cancelled_count'),
             F.avg(F.col('DISTANCE')).alias('avg_distance'),
             F.avg(F.col('AIR_TIME')).alias('avg_air_time'),
             F.count(F.when(F.col('CANCELLATION_REASON') == 'A', F.col('CANCELLATION_REASON'))).alias('airline_issue_count'),
             F.count(F.when(F.col('CANCELLATION_REASON') == 'B', F.col('CANCELLATION_REASON'))).alias('weather_issue_count'),
             F.count(F.when(F.col('CANCELLATION_REASON') == 'C', F.col('CANCELLATION_REASON'))).alias('nas_issue_count'),
             F.count(F.when(F.col('CANCELLATION_REASON') == 'D', F.col('CANCELLATION_REASON'))).alias('security_issue_count')
             ) \
        .select('*') \
        .orderBy(F.col('AIRLINE_NAME'))
        #res_df.withColumn('IS_DEPARTURE', res_df.when (F.col('DEPARTURE_DELAY') > 0)
    res_df.write.mode("overwrite").parquet(result_path)
    res_df.show(truncate=False, n=100)


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    # print(ROOT_DIR)
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path',
                        type=str,
                        default=str(ROOT_DIR) + '/flights.parquet',
                        help='Please set flights datasets path.')
    parser.add_argument('--airlines_path',
                        type=str,
                        default=str(ROOT_DIR) + '/airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--result_path',
                        type=str,
                        default=str(ROOT_DIR) + '/task_5/result',
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
