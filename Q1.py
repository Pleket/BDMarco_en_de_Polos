from pyspark.sql import SparkSession, DataFrame
from pyspark import RDD, SparkContext
from pyspark.sql.functions import split as pyspark_split
from pyspark.sql.types import ArrayType, IntegerType


class Q1:
    def __init__(self, person):
        self.person = person
    
    def solve_a(self, spark_context: SparkContext, on_server: bool, with_vector_type=True) -> DataFrame:

        return self.a_roelle(spark_context, on_server, with_vector_type)
        # match self.person:
        #     case "Roelle":
        #         return self.a_roelle(spark_context, on_server, with_vector_type)
        #     case "Lieke":
        #         return self.a_lieke(spark_context, on_server)
        #     case "Marco":
        #         return self.a_marco(spark_context, on_server)
        #     case "Rik":
        #         return self.a_rik(spark_context, on_server, with_vector_type)
        #     case "Dalton":
        #         return self.a_dalton(spark_context, on_server)
        #     case "Thomas":
        #         return self.a_thomas(spark_context, on_server)
    
    def solve_b(self, spark_context: SparkContext, on_server: bool) -> RDD:

        return self.b_roelle(spark_context, on_server)
        # match self.person:
        #     case "Roelle":
        #         return self.b_roelle(spark_context, on_server)
        #     case "Lieke":
        #         return self.b_lieke(spark_context, on_server)
        #     case "Marco":
        #         return self.b_marco(spark_context, on_server)
        #     case "Rik":
        #         return self.b_rik(spark_context, on_server)
        #     case "Dalton":
        #         return self.b_dalton(spark_context, on_server)
        #     case "Thomas":
        #         return self.b_thomas(spark_context, on_server)


    def a_roelle(self, spark_context: SparkContext, on_server: bool, with_vector_type=True) -> DataFrame:
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

    def a_lieke(self, spark_context: SparkContext, on_server: bool) -> DataFrame:
        vectors_file_path = "/vectors_groot.csv" if on_server else "vectors_groot.csv"

        spark_session = SparkSession(spark_context)

        # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.
        # df_pd = pd.read_csv(vectors_file_path, header=None)
        
        # df_numbers = df_pd[1]
        # print(df_numbers.head())
        df = spark_session.read.options(delimiter=",").csv(vectors_file_path)

    def a_marco(self, spark_context: SparkContext, on_server: bool) -> DataFrame:
        vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

        spark_session = SparkSession(spark_context)

        # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.

        return spark_session.read.csv(vectors_file_path, header=True)
    
    def a_rik(self, spark_context: SparkContext, on_server: bool, with_vector_type=True) -> DataFrame:
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
    
    def a_dalton(self, spark_context: SparkContext, on_server: bool) -> DataFrame:
        vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

        spark_session = SparkSession(spark_context)

        # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.
        df = spark_session.read.format("csv").load(vectors_file_path)

        return df
    
    def a_thomas(self, spark_context: SparkContext, on_server: bool) -> DataFrame:
        print("Not implemented")
    
    def b_roelle(self, spark_context: SparkContext, on_server: bool) -> RDD:
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
    
    def b_lieke(self, spark_context: SparkContext, on_server: bool) -> RDD:
        vectors_file_path = "/vectors_groot.csv" if on_server else "vectors_groot.csv"

        # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

        spark_session = SparkSession(spark_context)

        return spark_context.textFile(vectors_file_path)

    def b_marco(self, spark_context: SparkContext, on_server: bool) -> RDD:
        vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

        # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

        spark_session = SparkSession(spark_context)

        return spark_session.read.csv(vectors_file_path, header=True).rdd

    def b_rik(self, spark_context: SparkContext, on_server: bool, big=False) -> RDD:
        vectors_file_path = "/vectors.csv" if on_server else ("vectors_groot.csv" if big else 'vectors.csv')

        def split_row(row):
            key, values = row.split(',')
            t_values = tuple([int(value) for value in values.split(';')])
            # return tuple with key as first item and values after it
            return (key, t_values,)

        data = spark_context.textFile(vectors_file_path)
        return data.map(split_row)

    def b_dalton(self, spark_context: SparkContext, on_server: bool) -> RDD:
        vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

        # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.
        # rdd = spark_session.read.format("csv").load(vectors_file_path)
        rdd = spark_context.textFile(vectors_file_path)

        return rdd

    def b_thomas(self, spark_context: SparkContext, on_server: bool) -> RDD:
        print("Not implemented")