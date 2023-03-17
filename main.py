from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import ArrayType, IntegerType, FloatType, DoubleType
from pyspark.sql.functions import split as pyspark_split, col, udf, sum
from pyspark.ml.feature import VectorAssembler

from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import time

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


def q1a(spark_context: SparkContext, on_server: bool, with_vector_type=True) -> DataFrame:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"
    spark_session = SparkSession(spark_context)

    # read data from csv
    df = spark_session.read.options(delimiter=",").csv(vectors_file_path)

    # rename key column
    df = df.withColumn('key', df['_c0']).drop('_c0')

    global vec_dims
    vec_dims = len(df.take(1)[0]['_c1'].split(';'))
    print(f'{vec_dims =}')

    # split values and add them as vector
    split_col = pyspark_split(df['_c1'], ';').cast(ArrayType(IntegerType()))
    print(f'Split col {split_col}')


    # depending on the type used, either split over columns or within one column
    if with_vector_type:
        df = df.withColumn('vec', split_col)
    else:
        for i in range(vec_dims):
            df = df.withColumn(f'val_{i}', split_col.getItem(i))

    # remove old _c1 (; separated values)
    df = df.drop('_c1')

    df.registerTempTable('vectors')
    return df



def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"
    spark_session = SparkSession(spark_context)

    def split_row(row):
        key, values = row.split(',')
        t_values = tuple([int(value) for value in values.split(';')])
        # return tuple with key as first item and values after it
        return (key, t_values,)

    data = spark_context.textFile(vectors_file_path)
    data = data.map(split_row)
    rdd = data.map(lambda x: (x[0], list(x[1])))

    return rdd



def q2(spark_context: SparkContext, data_frame: DataFrame):
    # create UDF
    def get_variance_triple(a1, a2, a3):
        mu = 0
        total_sum_squared = 0
        for i in range(len(a1)):
            curr_sum = a1[i] + a2[i] + a3[i]
            mu += curr_sum / len(a1)
            total_sum_squared += curr_sum * curr_sum
        return 1/len(a1) * total_sum_squared - mu*mu

    sqlCtx = SQLContext(spark_context)

    sqlCtx.udf.register("VECVAR", get_variance_triple, FloatType())


    df_vec = sqlCtx.sql(
        f'''
                SELECT v1.key, v2.key, v3.key, VECVAR(v1.vec, v2.vec, v3.vec) AS variance
                FROM vectors as v1
                INNER JOIN vectors as v2 ON v1.key < v2.key
                INNER JOIN vectors as v3 ON v2.key < v3.key
                ORDER BY variance;
                ''')

    df_vec.registerTempTable('vectors_count')

    tau = [20,50,310,360,410]
    counts = []
    for i in tau:
        TAU_PARAMETER = i
        start_time = time.time()

        count = sqlCtx.sql(
            f'''
                SELECT * FROM vectors_count
                WHERE variance < {TAU_PARAMETER};
                ''').count()
        counts.append(count)
        # Print count of triple vectors
        print(f'$$ count {count}')
        # Get end time
        end_time = time.time()
        # Get and print execution time
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.4f} seconds, tau: {i}")


    # Create histogram
    fig, ax = plt.subplots()
    ax.hist(tau, weights=counts, bins=30)
    for c in ax.containers:
        ax.bar_label(c, labels=[v if v > 0 else '' for v in c.datavalues])
    ax.set_title('Amount of triple vectors with aggregate variance at most tau')
    ax.set_xlabel('Tau')
    ax.set_ylabel('Counts')
    plt.show()


def q3(spark_context: SparkContext, rdd: RDD):
    return


def q4(spark_context: SparkContext, rdd: RDD):
    return


if __name__ == '__main__':

    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
