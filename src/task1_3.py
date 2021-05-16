from task1_1 import read_to_rdd
from pyspark.context import SparkContext
from pyspark.rdd import RDD

def explode(row):
    for k in row:
        yield k

def list_top_purchased_products(rdd:RDD):
    """[task 3 - saves top 5 purchased products into txt file]

    Args:
        rdd (RDD): [spark RDD]
    """
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
