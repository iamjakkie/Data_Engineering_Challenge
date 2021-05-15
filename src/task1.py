import requests
import pyspark
import csv
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row

URL = 'https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv'
LOCAL_FILE = 'data/groceries.csv'


def download_groceries_data():
    url = URL
    r = requests.get(url)
    with open(LOCAL_FILE, 'wb') as f:
        f.write(r.content)

def read_to_rdd(path:str):
    sc = SparkContext.getOrCreate()
    rdd = sc.textFile(path)
    rdd = rdd.mapPartitions(lambda x: csv.reader(x))  
    return rdd

def explode(row):
    for k in row:
        yield k

def list_unique_products(rdd):
    unique = rdd.flatMap(explode).distinct()
    unique.coalesce(1).saveAsTextFile("out/out_1_2a.txt")

def list_product_count(rdd):
    sc = SparkContext.getOrCreate()
    rdd = sc.parallelize([str(rdd.flatMap(explode).count())])
    header = sc.parallelize(["Count:"])
    header.union(rdd).coalesce(1).saveAsTextFile("out/out_1_2b.txt")

def list_top_purchased_products(rdd):
    sc = SparkContext.getOrCreate()
    rdd = (sc.parallelize(rdd.flatMap(explode)
        .map(lambda w: (w,1))
        .reduceByKey(lambda a, b: a+b)
        #.sortBy(lambda a: -a[1])
        #.top(5, key=lambda x: x[1]))
        .takeOrdered(5, key=lambda x: -x[1]))
        .coalesce(1))
    rdd.saveAsTextFile("out/out_1_3.txt")
    

def main():
    #download_groceries_data()
    rdd = read_to_rdd(LOCAL_FILE)
    #list_unique_products(rdd)
    #list_product_count(rdd)
    list_top_purchased_products(rdd)



if __name__ == '__main__':
    main()