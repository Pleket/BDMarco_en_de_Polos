from pyspark import SparkContext, RDD
from pyspark.sql import DataFrame, SQLContext
from pyspark.sql.types import FloatType

import matplotlib.pyplot as plt
import time

def q2_sol(spark_context: SparkContext, data_frame: DataFrame):
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