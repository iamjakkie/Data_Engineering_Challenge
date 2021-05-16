from task3_1 import LOCAL_FILE
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col, when
from pyspark.sql.dataframe import DataFrame

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def get_schema() -> StructType:
    """[generate schema for DF]

    Returns:
        [StructType]: [spark structtype]
    """
    return StructType([StructField("sepal_length", DoubleType(), True),
            StructField("sepal_width", DoubleType(), True),
            StructField("petal_length", DoubleType(), True),
            StructField("petal_width", DoubleType(), True),
            StructField("class", StringType(), True)])

def load_data() -> DataFrame:
    """[loads data into dataframe]

    Returns:
        [DataFrame]: [spark dataframe]
    """
    schema = get_schema()
    return spark.read.csv(LOCAL_FILE, schema=schema)

def prepare_model(df: DataFrame) -> tuple:
    """[prepare pyspark model for logistic regression]

    Args:
        df ([DataFrame]): [spark dataframe]

    Returns:
        [tuple]: [tuple of vector assembler and lr model for predictions]
    """
    #label_indexer = StringIndexer(inputCol='class', outputCol='class_index')
    df = df.withColumn('class_index', when(col('class')=='Iris-setosa', 0).when(col('class')=='Iris-versicolor', 1).otherwise(2))
    vector_assembler = VectorAssembler(inputCols=['sepal_length',
                                            'sepal_width',
                                            'petal_length',
                                            'petal_width'], 
                                    outputCol='features')
    # stages = [vector_assembler]
    # pipeline = Pipeline(stages=stages)
    # pipelineModel = pipeline.fit(df)
    #transformed_df = pipelineModel.transform(df)
    transformed_df = vector_assembler.transform(df)
    selectedCols = ['features', 'class_index']
    final_df = transformed_df.select(selectedCols)
    log_reg = LogisticRegression(featuresCol='features',
                                labelCol='class_index',
                                regParam=100000)
    lrModel = log_reg.fit(final_df)
    return (vector_assembler, lrModel)
    # I have left some commented out code there - it could be improved in the future to 
    # use pipelining for predictions

def predict_class(df: DataFrame, vector_assembler :VectorAssembler, lrModel:LogisticRegression):
    """[Predict class of dataframe]

    Args:
        df (DataFrame): [spark dataframe for prediction]
        vector_assembler (VectorAssembler): [vector assembler to prepare features]
        lrModel (LogisticRegression): [model]
    """
    predict_df = vector_assembler.transform(df).select('features')
    predictions = lrModel.transform(predict_df)
    predictions = predictions.withColumn('class',when(col('prediction')==0, 'Iris-setosa').when(col('prediction')==1,'Iris-versicolor').otherwise('Iris-virginica'))
    (predictions.select('class')
        .coalesce(1)
        .write
        .option("header","true")
        .format('csv')
        .save('out/out_3_2.txt'))

def main():
    df = load_data()
    pipelineModel, lrModel = prepare_model(df)
    test_df = spark.createDataFrame(
        [(5.1, 3.5, 1.4, 0.2),
        (6.2, 3.4, 5.4, 2.3)],
        ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    )
    predict_class(test_df, pipelineModel, lrModel)

if __name__ == '__main__':
    main()
