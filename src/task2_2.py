from task2_1 import load_data
import pyspark.sql.functions as f

def calculate_prices(df):
    (df.agg(f.min('price').alias('min_price'),
            f.max('price').alias('max_price'),
            f.count(f.lit(1)).alias('row_count'))
        .coalesce(1)
        .write
        .option("header","true")
        .format('csv')
        .save('out/out_2_2.txt'))

def main():
    df = load_data()
    calculate_prices(df)

if __name__ == '__main__':
    main()