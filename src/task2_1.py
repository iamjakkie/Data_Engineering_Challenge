from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

spark = SparkSession \
    .builder \
    .appName("Truata") \
    .getOrCreate()

def load_data() -> DataFrame:
    """[task 1 - loads csv data into dataframe]

    Returns:
        [DataFrame]: [spark dataframe]
    """
    return spark.read.format('parquet').load('data/sf-airbnb-clean.parquet')

def main():
    df = load_data()
    print(df.show(5))

if __name__ == '__main__':
    main()