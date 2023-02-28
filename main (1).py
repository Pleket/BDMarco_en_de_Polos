from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import ArrayType, IntegerType, FloatType, DoubleType
from pyspark.sql.functions import split as pyspark_split, col, udf, sum

from pyspark.sql.functions import *
from pyspark.sql.types import *
from statistics import pvariance

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


def q1a(spark_context: SparkContext, on_server: bool, with_vector_type=False) -> DataFrame:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"
    spark_session = SparkSession(spark_context)


    return spark_session.read.csv(vectors_file_path, header=True)



def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"
    spark_session = SparkSession(spark_context)

    return spark_session.read.csv(vectors_file_path, header=True).rdd


def q2(spark_context: SparkContext, data_frame: DataFrame):

    data_frame.show()
    spark_session = SparkSession(spark_context)


    def agg_var(v1, v2, v3):
        agg_v1 = 0
        agg_v2 = 0
        agg_v3 = 0
        for i in range(1,len(v1)):
            agg_v1 = agg_v1 + v1[i]
            agg_v2 = agg_v2 + v2[i]
            agg_v3 = agg_v3 + v3[i]
            sum_agg = [agg_v1, agg_v2, agg_v3]
        mean = (agg_v1+agg_v2+agg_v3) / 3
        return sum((x - mean) ** 2 for x in sum_agg) / len(v1)
    #
    # var_udf = udf(var, DoubleType())
    # data_frame.select(col("val_0")).show()
    # vector_var = data_frame.select(col("vec"), var_udf(col("vec")).alias("var"))
    # vector_var.createOrReplaceTempView("vector_variances")

    # def calculate_var(row):
    #     data = [float(x.strip()) for x in row.split(",")]
    #     return pvariance(data)
    #
    # var_udf = udf(calculate_var, FloatType())
    # data_frame.withColumn('var',var_udf(concat_ws(",",data_frame.val_0, data_frame.val_1, data_frame.val_2, data_frame.val_3, data_frame.val_4, data_frame.val_5, data_frame.val_6, data_frame.val_7, data_frame.val_8, data_frame.val_9, data_frame.val_10, data_frame.val_11, data_frame.val_12, data_frame.val_13, data_frame.val_14, data_frame.val_15, data_frame.val_16, data_frame.val_17, data_frame.val_18, data_frame.val_19))).show()

    tau = 3
    triple_vectors = spark_session.sql(f"""
    SELECT v1.key as X, v2.key as Y, v3.key as Z, agg_var(v1.key, v2.key, v3.key) as aggreated
    FROM vector v1, vector v2, vector v3
    WHERE v1.vector != v2.vector AND v1.vector != v3.vector AND v2.vector != v3.vector
    GROUP BY v1.vector, v2.vector, v3.vector
    HAVING aggreated <= {tau}
    """)
    triple_vectors.show()
    return triple_vectors


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
    q1RDD = data_frame.rdd
    q1RDD.foreach(print)

    rdd = q1b(spark_context, on_server)
    print(rdd)

    q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
