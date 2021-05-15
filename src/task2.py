import requests
import pyspark
import csv
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from datetime import timedelta
import airflow 
from airflow import DAG 
from airflow.operators.dummy import DummyOperator

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def load_data():
    return spark.read.format('parquet').load('data/sf-airbnb-clean.parquet')

def csv_output(df):
    (df.agg(f.min('price').alias('min_price'),
            f.max('price').alias('max_price'),
            f.count(f.lit(1)).alias('row_count'))
        .coalesce(1)
        .write
        .option("header","true")
        .format('csv')
        .save('out/out_2_2.txt'))

def calculate_bathrooms_bedrooms(df):
    (df.filter((df.price > 5000) & (df.review_scores_value == 10))
        .agg(f.avg('bathrooms').alias('avg_bathrooms'),
            f.avg('bedrooms').alias('avg_bedrooms'))
        .coalesce(1)
        .write
        .option("header","true")
        .format('csv')
        .save('out/out_2_3.txt'))

def accomodate_people(df):
    (df.withColumn("rank_ranking",f.rank().over(Window.orderBy(f.desc("review_scores_value"))))
        .withColumn("price_ranking", f.rank().over(Window.orderBy("price")))
        .filter((f.col('rank_ranking') == 1) & (f.col("price_ranking")==1))
        .select(f.col("accommodates"))
        .coalesce(1)
        .write
        .option("header","true")
        .format('csv')
        .save('out/out_2_4.txt'))

def create_dag():
    args = {
        'owner': 'jakkie',
        'start_date': datetime(2021, 5, 14)
    }

    dag = DAG(
        'task5-dag',
        default_args=args,
        description='dag for task 5'
    )
    with dag:
        task1 = DummyOperator(task_id='Task_1')
        task2 = DummyOperator(task_id='Task_2')
        task3 = DummyOperator(task_id='Task_3')
        task4 = DummyOperator(task_id='Task_4')
        task5 = DummyOperator(task_id='Task_5')
        task6 = DummyOperator(task_id='Task_6')

        task1.set_downstream([task2, task3])
        task2.set_downstream([task4, task5, task6])
        task3.set_downstream([task4, task5, task6])

def main():
    df = load_data()
    #csv_output(df)
    #calculate_bathrooms_bedrooms(df)
    accomodate_people(df)

if __name__ == '__main__':
    main()