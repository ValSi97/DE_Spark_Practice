import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

ROOT_DIR = Path(os.getcwd()).parent.absolute()


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """
    airlines_df = spark.read.parquet(airlines_path) \
        .withColumnRenamed('AIRLINE', 'AIRLINE_NAME')

    airports_df = spark.read.parquet(airports_path)

    #airports_df_destination = airports_df_origin.alias('airports_df_destination')
    flights_df = spark.read.parquet(flights_path)

    res_df = airlines_df \
        .join(other=flights_df, on=flights_df['AIRLINE'] == airlines_df['IATA_CODE'], how='inner') \
        .join(other=airports_df.alias("airports_df_origin"), on=F.col('airports_df_origin.IATA_CODE') == F.col('ORIGIN_AIRPORT'), how='inner') \
        .withColumnRenamed('COUNTRY', 'ORIGIN_COUNTRY') \
        .withColumnRenamed('AIRPORT', 'ORIGIN_AIRPORT_NAME') \
        .withColumnRenamed('LATITUDE', 'ORIGIN_LATITUDE') \
        .withColumnRenamed('LONGITUDE', 'ORIGIN_LONGITUDE') \
        .join(other=airports_df.alias("airports_df_destination"),
              on=F.col('airports_df_destination.IATA_CODE') == F.col('DESTINATION_AIRPORT'),
              how='inner') \
        .withColumnRenamed('COUNTRY', 'DESTINATION_COUNTRY') \
        .withColumnRenamed('AIRPORT', 'DESTINATION_AIRPORT_NAME') \
        .withColumnRenamed('LATITUDE', 'DESTINATION_LATITUDE') \
        .withColumnRenamed('LONGITUDE', 'DESTINATION_LONGITUDE') \
        .select(F.col('AIRLINE_NAME'), F.col('TAIL_NUMBER'), F.col('ORIGIN_COUNTRY'), F.col('ORIGIN_AIRPORT_NAME'),
                F.col('ORIGIN_LATITUDE'), F.col('ORIGIN_LONGITUDE'), F.col('DESTINATION_COUNTRY'),
                F.col('DESTINATION_AIRPORT_NAME'), F.col('DESTINATION_LATITUDE'), F.col('DESTINATION_LONGITUDE')
                )

    res_df.write.mode("overwrite").parquet(result_path)


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path',
                        type=str,
                        default=str(ROOT_DIR) + '/flights.parquet',
                        help='Please set flights datasets path.')
    parser.add_argument('--airlines_path',
                        type=str,
                        default=str(ROOT_DIR) + '/airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--airports_path',
                        type=str,
                        default=str(ROOT_DIR) + '/airports.parquet',
                        help='Please set airports datasets path.')
    parser.add_argument('--result_path',
                        type=str,
                        default=str(ROOT_DIR) + '/task_4/result',
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
