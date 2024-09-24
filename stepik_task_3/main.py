import argparse

from py4j.protocol import DECIMAL_TYPE
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

from pyspark.sql.types import DecimalType


def main(actors_data_path, movie_actors_data_path, movies_data_path, result_path):
    spark = _spark_session()
    process(spark, actors_data_path, movie_actors_data_path, movies_data_path, result_path)

def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('Stepik_Task_1').getOrCreate()


def process(spark, actors_data_path, movie_actors_data_path, movies_data_path, result_path):
    spark.read.option("header",True).csv(actors_data_path) \
        .createOrReplaceTempView("actors")
    spark.read.option("header", True).csv(movie_actors_data_path) \
        .createOrReplaceTempView("movie_actors")
    spark.read.option("header", True).csv(movies_data_path) \
        .createOrReplaceTempView("movies")

    #Найдите топ-5 жанров по количеству фильмов.
    spark.sql("select genre, count(movie_id) as num_movies from movies "
              "group by genre "
              "order by num_movies desc "
              "limit 5") \
        .show()

    #Найдите актера с наибольшим количеством фильмов.
    spark.sql("select actors.name, count(movie_id) as num_movies "
              "from actors, movie_actors "
              "where actors.actor_id = movie_actors.actor_id "
              "group by actors.name "
              "order by num_movies desc "
              "limit 1"
              ) \
        .show()

    #Подсчитайте средний бюджет фильмов по жанрам.
    spark.sql("select genre, avg(budget) as avg_budget from movies "
              "group by genre "
              ) \
        .show()

    #Найдите фильмы, в которых снялось более одного актера из одной страны.
    spark.sql("select m.title, a.country, count(*) as num_actors "
              "from "
              "movies m, actors a, movie_actors ma "
              "where m.movie_id = ma.movie_id "
              "and a.actor_id = ma.actor_id "
              "group by m.title, a.country "
              "having num_actors > 1") \
        .show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--actors_data_path',
                        type=str,
                        default='./actors.csv')
    parser.add_argument('--movie_actors_data_path',
                        type=str,
                        default='./movie_actors.csv')
    parser.add_argument('--movies_data_path',
                        type=str,
                        default='./movies.csv')
    parser.add_argument('--result_path',
                        type=str,
                        default='./result',
                        help='Please set result path.')
    args = parser.parse_args()
    actors_data_path = args.actors_data_path
    movie_actors_data_path = args.movie_actors_data_path
    movies_data_path = args.movies_data_path
    result_path = args.result_path
    main(actors_data_path, movie_actors_data_path, movies_data_path, result_path)