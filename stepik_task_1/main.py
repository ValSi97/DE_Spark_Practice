import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

def main(weather_data_path, result_path):
    spark = _spark_session()
    process(spark, weather_data_path, result_path)

def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('Stepik_Task_1').getOrCreate()

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
    parser.add_argument('--weather_data_path',
                        type=str,
                        default='./weather_data.csv')
    parser.add_argument('--result_path',
                        type=str,
                        default='./result',
                        help='Please set result path.')
    args = parser.parse_args()
    weather_data_path = args.weather_data_path
    result_path = args.result_path
    main(weather_data_path, result_path)