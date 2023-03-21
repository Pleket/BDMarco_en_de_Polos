from pyspark import SparkConf, SparkContext, RDD
<<<<<<< Updated upstream
from pyspark.sql import DataFrame
=======
from pyspark.sql import SparkSession, DataFrame, SQLContext, functions
from pyspark.sql.types import ArrayType, IntegerType, FloatType
from pyspark.sql.functions import split as pyspark_split, col, posexplode
from time import sleep, time

# amount of values per vector (dimensions)
vec_dims = 20
>>>>>>> Stashed changes

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

<<<<<<< Updated upstream
def q1a(spark_context: SparkContext, on_server: bool, with_vector_type=True) -> DataFrame:
    return q1.solve_a(spark_context, on_server, with_vector_type)



def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    return q1.solve_b(spark_context, on_server)

=======
# def q1a(spark_context: SparkContext, on_server: bool, with_vector_type=False) -> DataFrame:
#     vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"
#     spark_session = SparkSession(spark_context)
#
#     # read data from csv
#     df = spark_session.read.options(delimiter=",").csv(vectors_file_path)
#
#     # rename key column
#     df = df.withColumn('key', df['_c0']).drop('_c0')
#
#     global vec_dims
#     vec_dims = len(df.take(1)[0]['_c1'].split(';'))
#     print(f'{vec_dims =}')
#
#     # split values and add them as vector
#     split_col = pyspark_split(df['_c1'], ';').cast(ArrayType(IntegerType()))
#     print(f'Split col {split_col}')
#
#     # depending on the type used, either split over columns or within one column
#     if with_vector_type:
#         df = df.withColumn('vec', split_col)
#     else:
#         for i in range(vec_dims):
#             df = df.withColumn(f'val_{i}', split_col.getItem(i))
#
#     # remove old _c1 (; separated values)
#     df = df.drop('_c1')
#
#     # temp table
#     df.registerTempTable('vectors')
#     return df

def q1a(spark_context: SparkContext, on_server: bool, with_vector_type=True) -> DataFrame:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # sparkSession = SparkSession.builder.sparkContext(spark_context.sc()).getOrCreate();

    sparkSession = SparkSession.builder.getOrCreate()

    # Read csv into dataframe
    df = sparkSession.read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "false") \
        .csv(vectors_file_path)

    # Rename
    df = df.toDF("key", "vec")
    # String to array
    df = df.withColumn("vec", functions.split(col("vec"), ";").cast("array<int>"))
    # Explode with positions
    df = df.select("key", posexplode("vec"))
    # Show
    df.show()

    return df


def q1b(spark_context: SparkContext, on_server: bool, big=False) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # sparkSession = SparkSession.builder.sparkContext(spark_context.sc()).getOrCreate();

    sparkSession = SparkSession.builder.getOrCreate()

    # Read csv into dataframe
    df = sparkSession.read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "false") \
        .csv(vectors_file_path)

    # Rename
    df = df.toDF("key", "vec")
    # String to array
    df = df.withColumn("vec", functions.split(col("vec"), ";").cast("array<int>"))
    # Explode with positions
    df = df.select("key", posexplode("vec"))
    # Show
    df.show()

    return df.rdd
>>>>>>> Stashed changes


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

<<<<<<< Updated upstream
=======
    from timeit import default_timer

    start = default_timer()
    print("Timer Started")
>>>>>>> Stashed changes
    q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
