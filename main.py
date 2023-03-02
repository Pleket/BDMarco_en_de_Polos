from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import ArrayType, IntegerType, FloatType
from pyspark.sql.functions import split as pyspark_split, broadcast
from time import sleep


# amount of values per vector (dimensions)
vec_dims = 20

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

    # temp table
    df.registerTempTable('vectors')
    return df


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    def split_row(row):
        key, values = row.split(',')
        t_values = tuple([int(value) for value in values.split(';')])
        # return tuple with key as first item and values after it
        return (key, t_values,)

    data = spark_context.textFile(vectors_file_path)
    return data.map(split_row)



def q2(spark_context: SparkContext, data_frame: DataFrame):
    # create UDF
    def get_variance(a1, a2, a3):
        mu         = 0 
        total_sum_squared = 0
        for i in range(len(a1)):
            curr_sum = a1[i] + a2[i] + a3[i]
            mu += curr_sum / len(a1)
            total_sum_squared += curr_sum * curr_sum
        return 1/len(a1) * total_sum_squared - mu*mu           

    # use it in query
    TAU_PARAMETER = 410

    sqlCtx = SQLContext(spark_context)
    sqlCtx.udf.register("VECVAR", get_variance, FloatType())
    count = sqlCtx.sql(
        f'''
        SELECT * FROM (
        SELECT v1.key, v2.key, v3.key, VECVAR(v1.vec, v2.vec, v3.vec) AS variance
        FROM vectors as v1 
        INNER JOIN vectors as v2 ON v1.key < v2.key
        INNER JOIN vectors as v3 ON v2.key < v3.key)
        WHERE variance < {TAU_PARAMETER};
        ''').count()

    print(f'$$ count {count}')


def q3(spark_context: SparkContext, rdd: RDD):
    TAU_PARAMETER = 410
    vecs = rdd.collect()
    vecs.sort(key=lambda item: item[0])
    vec_index = {vecs[i][0]: i for i in range(len(vecs))}

    # #BROADCASSTING
    # vi = spark_context.broadcast(vec_index)
    # vs = spark_context.broadcast(vecs)

    # print(vi)
    # vec_dims_inv = 1 / vec_dims
    # rdd = rdd.flatMap(lambda x: 
    #                     [(x[0],mapping[0], tuple(y[0]+y[1] for y in zip(x[1], mapping[1]))) for mapping in vs.value[vi.value[x[0]]+1: ] if vi.value[x[0]] != len(vs.value)-1]
    #                     ) \
    #             .flatMap(lambda x: 
    #                     [( (x[0], x[1], mapping[0]), tuple(y[0]+y[1] for y in zip(x[2], mapping[1]))) for mapping in vs.value[vi.value[x[1]]+1: ] if vi.value[x[1]] != len(vs.value)-1]
    #                     ) \
    #             .flatMap(lambda x:
    #                             [( x[0], (x[1][i] * 1/len(x[1]), x[1][i] * x[1][i])) for i in range(len(x[1]))]
    #                     ) \
    #             .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    #             .map(lambda row: (row[0], (vec_dims_inv * row[1][1]) - (row[1][0] * row[1][0])))\
    #             .filter(lambda row: row[1] < TAU_PARAMETER) \

    vec_dims_inv = 1 / vec_dims
    rdd = rdd.flatMap(lambda x: 
                        [(x[0],mapping[0], tuple(y[0]+y[1] for y in zip(x[1], mapping[1]))) for mapping in vecs[vec_index[x[0]]+1: ] if vec_index[x[0]] != len(vecs)-1]
                        ) \
                .flatMap(lambda x: 
                        [( (x[0], x[1], mapping[0]), tuple(y[0]+y[1] for y in zip(x[2], mapping[1]))) for mapping in vecs[vec_index[x[1]]+1: ] if vec_index[x[1]] != len(vecs)-1]
                        ) \
                .flatMap(lambda x:
                                [( x[0], (x[1][i] * 1/len(x[1]), x[1][i] * x[1][i])) for i in range(len(x[1]))]
                        ) \
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                .map(lambda row: (row[0], (vec_dims_inv * row[1][1]) - (row[1][0] * row[1][0])))\
                .filter(lambda row: row[1] < TAU_PARAMETER) \

    #print(f">> {rdd.collect()}")
    print(f">>COUNT {rdd.count()}")
    return
    
def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q4 here
    return

if __name__ == '__main__':
    try:
        on_server = False  # TODO: Set this to true if and only if deploying to the server
        spark_context = get_spark_context(on_server)

        data_frame = q1a(spark_context, on_server, with_vector_type=True)

        rdd = q1b(spark_context, on_server)

        #q2(spark_context, data_frame)

        q3(spark_context, rdd)

        #q4(spark_context, rdd)

        sleep(100)
    except Exception as e:
        print(e)
        spark_context.stop()
