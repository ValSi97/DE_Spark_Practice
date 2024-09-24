import argparse

from unicodedata import decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

def main(web_server_logs_path, result_path):
    spark = _spark_session()
    web_server_df = spark.read.option("header",True).csv(web_server_logs_path).cache()

    #1. Сгруппируйте данные по IP и посчитайте количество запросов для каждого IP, выводим 10 самых активных IP.
    top_10_ip_reqs_cnt = web_server_df[['ip']].groupby(F.col('ip')) \
        .agg(F.count('*').alias('request_count')) \
        .orderBy(F.col('request_count').desc()) \
        .limit(10) \
        .show()

    #2. Сгруппируйте данные по HTTP-методу и посчитайте количество запросов для каждого метода.
    method_reqs_cnt = web_server_df[['method']].groupby(F.col('method')) \
        .agg(F.count('*').alias('method_count')) \
        .orderBy(F.col('method_count').desc()) \
        .show()

    #3. Профильтруйте и посчитайте количество запросов с кодом ответа 404.
    reqs_404_cnt = web_server_df[['ip', 'response_code']] \
        .where(F.col('response_code') == 404) \
        .select(F.count('ip')).collect()[0][0]

    print(f'Number of records with 404 code {reqs_404_cnt}')

    #4. Сгруппируйте данные по дате и просуммируйте размер ответов, сортируйте по дате.
    sum_resp_size_by_date = web_server_df[['timestamp', 'response_size']] \
        .withColumn("date", F.to_date('timestamp')) \
        .withColumn('response_size', web_server_df['response_size'].cast('int')) \
        .groupby('date') \
        .agg(F.sum(F.col('response_size'))) \
        .orderBy(F.col('date').asc()) \
        .show()


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('Stepik_Task_4').getOrCreate()

def process(spark, weather_data_path, result_path):
    weather_df = spark.read.option("header",True).csv(weather_data_path)
    weather_df = weather_df.withColumn("date", F.to_date(weather_df["date"])) \
                .withColumn("temperature",weather_df["temperature"].cast("float")) \
                .withColumn("wind_speed", weather_df["wind_speed"].cast("float")) \
                .withColumn("precipitation", weather_df["precipitation"].cast("float"))
    print(weather_df.dtypes)
    weather_df.show(10)

    # Получение списка колонок с пропущенными значениями
    missing_columns = [col_name for col_name in weather_df.columns if weather_df.filter(F.col(col_name).isNull()).count() > 0]
    print(missing_columns)

    if missing_columns:
        for col_name in missing_columns:
            mean_value = weather_df.select(F.mean(F.col(col_name))).collect()[0][0]
            print(col_name, mean_value)
            weather_df = weather_df.na.fill({col_name: mean_value})

    top_5_hottest_days_df = weather_df[['date', 'temperature']].orderBy(F.col('temperature').desc()) \
        .limit(5).show()

    last_year = weather_df[['date']].select(F.max(F.col('date'))).collect()[0][0].year

    top_1_station_prec = weather_df[['station_id', 'date', 'precipitation']] \
        .filter(F.year(F.col('date')) == last_year) \
        .groupby(['station_id']).agg(F.sum('precipitation').alias('sum_prec')) \
        .orderBy(F.col('sum_prec').desc()) \
        .limit(1) \
        .show()
    avg_month_temp = weather_df.select(F.col('*'), F.month(F.col('date')).alias('month')) \
        .groupby(F.col('month')) \
        .agg(F.avg(F.col('temperature')).alias('avg_temp')) \
        .orderBy(F.col('month')) \
        [['month', 'avg_temp']] \
        .show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--web_server_logs_path',
                        type=str,
                        default='./web_server_logs.csv')
    parser.add_argument('--result_path',
                        type=str,
                        default='./result',
                        help='Please set result path.')
    args = parser.parse_args()
    web_server_logs_path = args.web_server_logs_path
    result_path = args.result_path
    main(web_server_logs_path, result_path)