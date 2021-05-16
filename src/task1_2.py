from task1_1 import read_to_rdd
from pyspark.context import SparkContext

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

def main():
    rdd = read_to_rdd()
    list_unique_products(rdd) #2a
    list_product_count(rdd) #2b

if __name__ == '__main__':
    main()