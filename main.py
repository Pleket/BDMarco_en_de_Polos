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

    return None


def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q3 here

    def compute_variance(line: str):
        len_X = len(line)

        mu_X = 0 
        quadraticSum = 0
        for value in line:
            value_int = value
            intermediate = value_int / len_X
            mu_X += intermediate
        
            quad = value_int**2
            quadraticSum += quad
        
        quadratic = quadraticSum / len_X
        variance = quadratic - mu_X**2

        return variance
        
    #Get RDD in form [['val', 'nr'], [ , ], [ , ] ]
    rdd_splitted = rdd.map( lambda line: line.split(","))

    #Get RDD combined with three values
    #TODO: Figure out quicker and more efficient way. Maybe loop over all other pairs or cartesian once?
    #TODO: Also, (A, B, C), (A, C, B) filter needed
    rdd_combine = rdd_splitted.cartesian(rdd_splitted).filter(lambda line: line[0][0] != line[1][0])\
                              .cartesian(rdd_splitted).map(lambda line: (line[0][0], line[0][1], line[1]))\
                              .filter(lambda line: line[0][0] != line[2][0] and line[1][0] != line[2][0])
    
    #Find the aggregated vectors
    def aggregated_vecs(line):
        line1 = line[0]
        line2 = line[1]
        line3 = line[2]

        val1 = line1[1].split(";")
        val2 = line2[1].split(";")
        val3 = line3[1].split(";")

        combi_name = (line1[0], line2[0], line3[0])

        len_X = len(val1)

        newVals = []
        for i in range(len_X):
            value = int(val1[i]) + int(val2[i]) + int(val3[i])

            newVals.append(value)
        
        return (combi_name, newVals)
    
    rdd_vars = rdd_combine.map(lambda line: aggregated_vecs(line))\
                          .map(lambda line: (line[0], compute_variance(line[1])))
    
    rdd_under_410 = rdd_vars.filter(lambda line: line[1] <= 410)
    #rdd_under_20 = rdd_under_410.filter(lambda line: line[1] <= 20)

    print(rdd_under_410.take(2))
    #print(rdd_under_20.collect())

    return None


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q4 here
    return None



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
