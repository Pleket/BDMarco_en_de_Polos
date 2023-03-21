from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import DataFrame

from Q1 import q1_sol_a, q1_sol_b
from Q2 import q2_sol
from Q3 import q3_sol
from Q4 import Q4

q4 = Q4(0.01, 0.1)

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

def q1a(spark_context: SparkContext, on_server: bool, with_pos: bool, with_vector_type=True) -> DataFrame:
    return q1_sol_a(spark_context, on_server, with_pos, with_vector_type)



def q1b(spark_context: SparkContext, on_server: bool, with_pos: bool) -> RDD:
    return q1_sol_b(spark_context, with_pos, on_server)



def q2(spark_context: SparkContext, data_frame: DataFrame):
    q2_sol(spark_context, data_frame)


def q3(spark_context: SparkContext, rdd: RDD):
    q3_sol(spark_context, rdd)


def q4_sol(spark_context: SparkContext, rdd: RDD):
    return q4.solve(spark_context, rdd)


if __name__ == '__main__':

    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server, with_pos=False)

    rdd = q1b(spark_context, on_server, with_pos=False)

    #q2(spark_context, data_frame)

    #q3(spark_context, rdd)

    q4_sol(spark_context, rdd)

    spark_context.stop()
