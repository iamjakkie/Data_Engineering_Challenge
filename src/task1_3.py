from task1_1 import read_to_rdd
from pyspark.context import SparkContext

def explode(row):
    for k in row:
        yield k

def list_top_purchased_products(rdd):
    sc = SparkContext.getOrCreate()
    rdd = (sc.parallelize(rdd.flatMap(explode)
        .map(lambda w: (w,1))
        .reduceByKey(lambda a, b: a+b)
        .takeOrdered(5, key=lambda x: -x[1]))
        .coalesce(1))
    rdd.saveAsTextFile("out/out_1_3.txt")

def main():
    rdd = read_to_rdd()
    list_top_purchased_products(rdd)

if __name__ == '__main__':
    main()