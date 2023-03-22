from pyspark.sql import SparkSession, DataFrame
from pyspark import RDD, SparkContext
from pyspark.sql.functions import split as col, posexplode
from pyspark.sql import SparkSession, DataFrame, functions


def q1_sol_a(spark_context: SparkContext, on_server: bool, with_pos: bool, with_vector_type=True) -> DataFrame:
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
    df = df.withColumn("vec", functions.split("vec", ";").cast("array<int>"))
    # Explode with positions
    if (with_pos):
        df = df.select("key", posexplode("vec"))
    # Show
    df.show()

    return df

def q1_sol_b(spark_context: SparkContext, on_server: bool, with_pos: bool, big=False) -> RDD:
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
    df = df.withColumn("vec", functions.split("vec", ";").cast("array<int>"))
    # Explode with positions
    if (with_pos):
        df = df.select("key", posexplode("vec"))
    # Show
    df.show()

    return df.rdd