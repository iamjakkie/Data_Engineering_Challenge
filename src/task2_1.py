from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def load_data():
    return spark.read.format('parquet').load('data/sf-airbnb-clean.parquet')

def main():
    df = load_data()
    print(df.show(5))

if __name__ == '__main__':
    main()