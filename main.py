from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, functions
import pandas as pd


def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        # log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("ERROR")

    return spark_context


def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    spark_session = SparkSession(spark_context)

    # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.
    # df_pd = pd.read_csv(vectors_file_path, header=None)
    
    # df_numbers = df_pd[1]
    # print(df_numbers.head())
    df = spark_session.read.options(delimiter=",").csv(vectors_file_path)
    return df


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    spark_session = SparkSession(spark_context)

    return spark_context.textFile(vectors_file_path)


def q2(spark_context: SparkContext, data_frame: DataFrame):
    # TODO: Imlement Q2 here
    #Create more logical column names
    data_frame = data_frame.select(functions.col("_c0").alias("code"), functions.col("_c1").alias("number"))
    data_frame.show()

    return None


def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q3 here
    return


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q4 here
    return


if __name__ == '__main__':

    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server)
    print(data_frame)

    rdd = q1b(spark_context, on_server)
    print(rdd)

    q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
