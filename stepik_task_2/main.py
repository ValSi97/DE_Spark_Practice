import argparse

from py4j.protocol import DECIMAL_TYPE
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path

from pyspark.sql.types import DecimalType


def main(authors_data_path, books_data_path, result_path):
    spark = _spark_session()
    process(spark, authors_data_path,books_data_path, result_path)

def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('Stepik_Task_1').getOrCreate()

def process(spark, authors_data_path, books_data_path, result_path):
    authors_df = spark.read.option("header",True).csv(authors_data_path)
    books_df = spark.read.option("header", True).csv(books_data_path)

    authors_df = authors_df.withColumn('birth_date', F.to_date(authors_df['birth_date'])).cache()
    books_df = books_df.withColumn('publish_date', F.to_date(books_df['publish_date'])) \
        .withColumn('price', F.col('price').cast(DecimalType(5,2))).cache()

    print(authors_df.dtypes)
    print(books_df.dtypes)

    authors_df.show(10)
    books_df.show(10)

    authors_books_df = books_df.join(other=authors_df,
                                     on=['author_id'],
                                     how='inner').cache()
    authors_books_df.show(20)

    #Найдите топ-5 авторов, книги которых принесли наибольшую выручку.
    top_5_authors = authors_books_df[['author_id', 'name', 'price']] \
        .groupby(F.col('author_id'), F.col('name')) \
        .agg(F.sum('price').alias('total_revenue')) \
        .orderBy(F.col('total_revenue').desc()) \
        .select(F.col('author_id'), F.col('name'), F.col('total_revenue')) \
        .limit(5) \
        .show()

    #Найдите количество книг в каждом жанре.
    genre_cnt_df = books_df.groupby(F.col('genre')) \
        .agg(F.count('genre').alias('count')) \
        .orderBy(F.col('count').desc()) \
        .show()

    #Подсчитайте среднюю цену книг по каждому автору.
    avg_book_price_df = authors_books_df[['author_id', 'name', 'price']] \
        .groupby(F.col('author_id'), F.col('name')) \
        .agg(F.avg(F.col('price')).alias('average_price')) \
        .orderBy(F.col('average_price').desc()) \
        .show()

    # Найдите книги, опубликованные после 2000 года, и отсортируйте их по цене.
    books_after_2000_df = authors_books_df.filter(F.year(F.col('publish_date')) >= 2000) \
        .orderBy(F.col('price').desc()) \
        .show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--authors_data_path',
                        type=str,
                        default='./authors.csv')
    parser.add_argument('--books_data_path',
                        type=str,
                        default='./books.csv')
    parser.add_argument('--result_path',
                        type=str,
                        default='./result',
                        help='Please set result path.')
    args = parser.parse_args()
    authors_data_path = args.authors_data_path
    books_data_path = args.books_data_path
    result_path = args.result_path
    main(authors_data_path, books_data_path, result_path)