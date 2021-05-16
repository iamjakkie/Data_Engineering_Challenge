import pytest
import pandas as pd
from task1_1 import read_to_rdd
from task1_2 import list_unique_products, list_product_count
from task1_3 import list_top_purchased_products
from task2_1 import load_data
from pyspark.sql import SparkSession
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
import os

spark = SparkSession \
    .builder \
    .appName("Truata") \
    .getOrCreate()

def test_read_rdd():
    rdd = read_to_rdd()
    assert(not rdd.isEmpty())
    assert(type(rdd) == RDD)

def test_rdd_conent():
    rdd = read_to_rdd()
    expected = [['citrus fruit', 'semi-finished bread', 'margarine', 'ready soups'],
                ['tropical fruit', 'yogurt', 'coffee'],
                ['whole milk'],
                ['pip fruit', 'yogurt', 'cream cheese ', 'meat spreads'],
                ['other vegetables',
                'whole milk',
                'condensed milk',
                'long life bakery product']]
    assert(rdd.take(5) == expected)

def test_unique_products():
    try:
        df = pd.read_csv('out/out_1_2a.txt/part-00000', delimiter='\t', header=None)
        assert(df.shape[0] > 0 and df.shape[1] == 1)
    except Exception as e:
        assert False, 'File could not be read'

def test_product_count():
    try:
        df = pd.read_csv('out/out_1_2b.txt/part-00000', delimiter='\t')
        assert(df.shape[0] > 0 and df.shape[1] == 1)
        assert(df.columns[0] == 'Count:')
    except Exception as e:
        assert False, 'File could not be read'

def test_top_purchased_products():
    try:
        df = pd.read_csv('out/out_1_3.txt/part-00000', delimiter='\t', header=None)
        assert(df.shape == (5,1))
    except Exception as e:
        assert False, 'File could not be read'

def test_load_df():
    df = load_data()
    assert(type(df) == DataFrame)
    assert(df.count() > 0 and len(df.columns) > 0)

def test_calculated_prices():
    try:
        df = spark.read.format('csv').option('header','true').load('out/out_2_2.txt')
        assert(df.columns == ['min_price', 'max_price', 'row_count'])
        assert(df.count() == 1)
    except Exception as e:
        assert False, 'File could not be read'

def test_calculated_bathrooms():
    try:
        df = spark.read.format('csv').option('header','true').load('out/out_2_3.txt')
        assert(df.columns == ['avg_bathrooms', 'avg_bedrooms'])
        assert(df.count() == 1)
    except Exception as e:
        assert False, 'File could not be read'

def test_accomodate_people():
    try:
        df = spark.read.format('csv').option('header', 'true').load('out/out_2_4.txt')
        assert(df.columns[0] == 'accommodates')
        assert(df.count() == 1)
    except Exception as e:
        assert False, 'File could not be read'

def test_iris_data():
    try:
        df = pd.read_csv('data/iris.csv', header=None, names=['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class'])
        assert(df.shape == (150,5))
    except Exception as e:
        assert False, 'File could not be read'

def test_predictions():
    try:
        df = spark.read.format('csv').option('header', 'true').load('out/out_3_2.txt')
        assert(df.count() == 2)
        assert(df.columns[0] == 'class')
        pdf = df.toPandas()
        assert(pdf['class'][0] == 'Iris-setosa')
        assert(pdf['class'][1] == 'Iris-virginica')
    except Exception as e:
        assert False, 'File could not be read'