from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import DataFrame
from time import sleep

from Q1 import Q1
from Q2 import q2_sol
from Q3 import q3_sol

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

q1 = Q1("Roelle")

def q1a(spark_context: SparkContext, on_server: bool, with_vector_type=True) -> DataFrame:
    return q1.solve_a(spark_context, on_server, with_vector_type)



def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    return q1.solve_b(spark_context, on_server)



def q2(spark_context: SparkContext, data_frame: DataFrame):
    q2_sol(spark_context, data_frame)


def q3(spark_context: SparkContext, rdd: RDD):
    q3_sol(spark_context, rdd)


def q4(spark_context: SparkContext, rdd: RDD):
    return


if __name__ == '__main__':

    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    #q2(spark_context, data_frame)

    q3(spark_context, rdd)

    #q4(spark_context, rdd)
    sleep(10000)

    spark_context.stop()
