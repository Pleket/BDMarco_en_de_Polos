from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame


def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        # log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("WARN")

    return spark_context


def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    spark_session = SparkSession(spark_context)

    # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.

    # read input text file to csv
    # TODO: get the form right. Need enough columns
    df = spark_session.read.options(delimiter=',').csv(vectors_file_path)

    return df


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    # create Spark context
    sc = get_spark_context(on_server=False)
 
    # read input text file to RDD
    # # TODO: get the form right. Need enough columns

    lines = sc.textFile(vectors_file_path)

    return lines


def q2(spark_context: SparkContext, data_frame: DataFrame):
    # TODO: Imlement Q2 here
    # Dataset is already inputted.



    return


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

    print(rdd.take(10))

    q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
