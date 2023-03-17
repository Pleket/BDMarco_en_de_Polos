from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark.sql.types import ArrayType, IntegerType, FloatType
from pyspark.sql.functions import split as pyspark_split, posexplode
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
    df = df.withColumn('vec', split_col)

    # remove old _c1 (; separated values)
    df = df.drop('_c1')

    # add vec
    df = df.select('key', posexplode('vec'))

    # temp table
    df = df.repartition(10)
    df.registerTempTable('vectors')

    df.show()
    return df


def q1b(spark_context: SparkContext, on_server: bool, big=False) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else ("vectors_groot.csv" if big else 'vectors.csv')

    def split_row(row):
        key, values = row.split(',')
        t_values = tuple([int(value) for value in values.split(';')])
        # return tuple with key as first item and values after it
        return (key, t_values,)

    data = spark_context.textFile(vectors_file_path)
    return data.map(split_row)



def q2(spark_context: SparkContext, data_frame: DataFrame):
    # create UDF
    def get_variance_triple(a1, a2, a3):
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
    #sqlCtx.udf.register("VECVAR", get_variance_triple, FloatType())
    count = sqlCtx.sql(
        f'''
        SELECT triple_id, VAR_POP(col) FROM (
        SELECT CONCAT(v1.key, ',', v2.key, ',', v3.key) AS triple_id, v1.pos, v1.col + v2.col + v3.col as col
        FROM vectors as v1 
        INNER JOIN vectors as v2 ON (v1.key < v2.key AND v1.pos = v2.pos)
        INNER JOIN vectors as v3 ON (v2.key < v3.key AND v2.pos = v3.pos))
        GROUP BY triple_id
        HAVING VAR_POP(col) < {TAU_PARAMETER};
        ''').count()

    print(f'$$ count {count}')


def q3(spark_context: SparkContext, rdd: RDD):
    def agg_var(agg_vec):
            som = 0
            som_squared = 0

            for value in agg_vec:
                som += value
                som_squared += value*value

            length_vector = len(agg_vec)
            avg = 1 / length_vector * som
            avg_sq = avg*avg
            sq_som = 1 / length_vector * som_squared
            
            return sq_som - avg_sq

    def agg_vec1(line, vs, vi):
        
        mapping = [( (line[0], line2[0]), tuple(nr[0] + nr[1] for nr in zip(line[1], line2[1]))) 
                    for line2 in vs.value[ vi.value [line[0]] + 1:] 
                    if vi.value[ line[0]] != len(vs.value)-1]

        return mapping
    
    def agg_vec2(line, vs, vi):
        
        mapping = [( line[0][0] + line[0][1] + line3[0], 
                    tuple(nr[0] + nr[1] for nr in zip(line[1], line3[1]))) 
                    for line3 in vs.value[ vi.value[ line[0][1] ] + 1 : ] 
                    if vi.value[ line[0][1] ] != len(vs.value)-1]

        return mapping

    #Get the vectors from the RDD and sort the values in the vector
    vecs = rdd.collect()
    vecs.sort(key=lambda item: item[0])

    #Take the index only
    vec_index = {vecs[i][0]: i for i in range(len(vecs))}

    #Use broadcasting to store the names of the vectors
    vi = spark_context.broadcast(vec_index)
    vs = spark_context.broadcast(vecs)

    #Split the data into partitions to distribute the work load
    rdd = rdd.repartition(8).flatMap(lambda line: agg_vec1(line, vs, vi))\
                            .flatMap(lambda line: agg_vec2(line, vs, vi))\
                            .map(lambda line: (line[0], agg_var(line[1])))
    
    rdd_410 = rdd.filter(lambda line: line[1] < 410)
    rdd_20 = rdd_410.filter(lambda line: line[1] < 20)
          
    #print(f">> {rdd.collect()}")
    print(f">>COUNT {rdd_410.count()}")
    print(f">>COUNT {rdd_20.count()}")
    return
    
def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q4 here
    return

if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server
    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server, with_vector_type=True)

    rdd = q1b(spark_context, on_server, big=False)

    q2(spark_context, data_frame)

    #q3(spark_context, rdd)

    #q4(spark_context, rdd)

    sleep(10000)
    spark_context.stop()

