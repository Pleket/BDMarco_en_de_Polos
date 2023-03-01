from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import ArrayType, IntegerType, FloatType
from pyspark.sql.functions import split as pyspark_split, col, udf


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

    return spark_context.textFile(vectors_file_path).map(split_row)


# def q2_old(spark_context: SparkContext, data_frame: DataFrame):
#     tau = 50

#     # create a data frame that contains all combinations between vectors of size 3
#     # and make sure they are unique, i.e (AA, BB, CC) == (CC, BB, AA) so second is not included
#     cross = data_frame.alias('d1') \
#             .join(data_frame.alias('d2'), col('d1.key') < col('d2.key')) \
#             .join(data_frame.alias('d3'), col('d2.key') < col('d3.key')) \

#     def get_col_sums(df):
#         # initialize sums to 0
#         total_sum = 0
#         total_sum_squared = 0
#         n_columns = vector_dimensions + 1 # amount of values plus key

#         # iterate over columns only once
#         for i, curr_name in enumerate(df.columns):
#             # do not consider keys
#             if curr_name == 'key':
#                 continue

#             # determines to which of 3 vectors the column belongs
#             idx = (i // n_columns)+1
#             if idx > 1:
#                 # idx 2 and 3 are already dealt with below
#                 break 

#             # update column sums
#             curr_sum = df[f'd1.{curr_name}'] + df[f'd2.{curr_name}'] + df[f'd3.{curr_name}']
#             total_sum += curr_sum
#             total_sum_squared  += curr_sum * curr_sum
#         return total_sum, total_sum_squared

#     # add sum and sum squared to determine the variance after
#     print(get_col_sums(cross))
#     col_sums = get_col_sums(cross)
#     moments = cross.withColumn('sum',  col_sums[0]) \
#                    .withColumn('sum2', col_sums[1]) \
#                    .withColumn('var',  1/vector_dimensions * (col('sum2') - 1/vector_dimensions * col('sum') * col('sum')))

#     # select relevant variances (< tau)
#     result = moments.select('d1.key', 'd2.key', 'd3.key', 'sum', 'sum2', 'var')
#     print('result')
#     result.show()
#     return result

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
    result = sqlCtx.sql(
        f'''
        SELECT * FROM (
        SELECT v1.key, v2.key, v3.key, VECVAR(v1.vec, v2.vec, v3.vec) AS variance
        FROM vectors as v1 
        INNER JOIN vectors as v2 ON v1.key < v2.key
        INNER JOIN vectors as v3 ON v2.key < v3.key)
        WHERE variance < {TAU_PARAMETER};
        ''')

    collection = result.count()
    print(f'$$ count {collection}')


def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q3 here

    # row[0][0] is the key of the first  rdd after cartesian
    # row[1][0] is the key of the second rdd after cartesian
    rdd_cartesian_join1 = rdd.cartesian(rdd).filter(lambda row: row[0][0] < row[1][0]) \
        .map(lambda row: ((row[0][0], row[1][0]), (row[0][1], row[1][1])))

    # structure is now row[0] = tuple of two keys
    #                  row[1] = tuple of two value lists (also tuple typed)

    # row[0][0][1] is the key of the second rdd after cartesian
    # row[1][0]    is the key of the third  rdd after cartesian
    rdd_cartesian_join2 = rdd_cartesian_join1.cartesian(rdd).filter(lambda row: row[0][0][1] < row[1][0]) \
        .map(lambda row: (row[0][0] + (row[1][0],),  # first we merge keys of 1 and 2 with key of 3
         (row[0][1] + (row[1][1],))))                # then we merge the values of 1 and 2 with values of 3

    for x in rdd_cartesian_join2.take(10):
        print(x)

    # map 
    def get_sums(row):
        sums = [0, 0] # total sum and total sum of squares
        for i in range(len(row[1][0])): # loop over first vector values
            sums[0] += row[1][0][i]  + row[1][1][i] + row[1][2][i]
            sums[1] += (row[1][0][i] + row[1][1][i] + row[1][2][i]) ** 2
        return (row[0], 1/vec_dims * (sums[1] - 1/vec_dims * sums[0] * sums[0]),)

    variances = rdd_cartesian_join2.map(get_sums)
    for x in variances.take(10):
        print(x)


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q4 here
    return

if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server, with_vector_type=True)

    rdd = q1b(spark_context, on_server)

    q2(spark_context, data_frame)

    #q3(spark_context, rdd)

    #q4(spark_context, rdd)

    spark_context.stop()
