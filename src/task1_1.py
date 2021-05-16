import csv
import requests
from pyspark.context import SparkContext
from pyspark.rdd import RDD

URL = 'https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv'
LOCAL_FILE = 'data/groceries.csv'


def download_groceries_data():
    """[function to download data]
    """
    url = URL
    response = requests.get(url)
    with open(LOCAL_FILE, 'wb') as f:
        f.write(response.content)

def read_to_rdd() -> RDD:
    """[read file into RDD]

    Returns:
        RDD: [pyspark RDD]
    """
    sc = SparkContext.getOrCreate()
    rdd = sc.textFile(LOCAL_FILE)
    rdd = rdd.mapPartitions(lambda x: csv.reader(x))
    return rdd

def main():
    download_groceries_data()
    rdd = read_to_rdd()
    print(rdd.take(5))

if __name__ == '__main__':
    main()
