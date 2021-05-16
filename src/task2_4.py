import pyspark.sql.functions as f
from task2_1 import load_data
from pyspark.sql.window import Window


def accomodate_people(df):
    (df.withColumn("rank_ranking",f.rank().over(Window.orderBy(f.desc("review_scores_value"))))
        .withColumn("price_ranking", f.rank().over(Window.orderBy("price")))
        .filter((f.col('rank_ranking') == 1) & (f.col("price_ranking")==1))
        .select(f.col("accommodates"))
        .coalesce(1)
        .write
        .option("header","true")
        .format('csv')
        .save('out/out_2_4.txt'))

def main():
    df = load_data()
    accomodate_people(df)

if __name__ == '__main__':
    main()