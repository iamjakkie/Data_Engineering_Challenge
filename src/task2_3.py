from task2_1 import load_data
import pyspark.sql.functions as f

def calculate_bathrooms_bedrooms(df):
    (df.filter((df.price > 5000) & (df.review_scores_value == 10))
        .agg(f.avg('bathrooms').alias('avg_bathrooms'),
            f.avg('bedrooms').alias('avg_bedrooms'))
        .coalesce(1)
        .write
        .option("header","true")
        .format('csv')
        .save('out/out_2_3.txt'))

def main():
    df = load_data()
    calculate_bathrooms_bedrooms(df)

if __name__ == '__main__':
    main()