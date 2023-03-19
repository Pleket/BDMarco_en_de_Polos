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


    df = sqlCtx.sql(
        f'''
                SELECT v1.key, v2.key, v3.key, VECVAR(v1.vec, v2.vec, v3.vec) AS variance
                FROM vectors as v1
                INNER JOIN vectors as v2 ON v1.key < v2.key
                INNER JOIN vectors as v3 ON v2.key < v3.key
                ORDER BY variance;
                ''')
    df_vec = df.withColumn("variance_idx", col("variance")).sort("variance")
    df_vec.repartition(10).registerTempTable('vectors_count_0')

    # Begint 410 dat de rest er onder valt
    tau = [410, 360, 310, 50, 20]
    counts = []
    for i in range(0,len(tau)):
        TAU_PARAMETER = tau[i]
        start_time = time.time()

        df_tau = sqlCtx.sql(
            f'''
                SELECT variance FROM vectors_count_{i}
                WHERE variance < {TAU_PARAMETER};
                ''')
        temp = f'vectors_count_{i+1}'
        df_tau.repartition(10).createOrReplaceTempView(temp)
        count = df_tau.count()
        counts.append(count)
        # Print count of triple vectors
        print(f'$$ count {count}')
        # Get end time
        end_time = time.time()
        # Get and print execution time
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.4f} seconds, tau: {tau[i]}")


    # Create histogram
    fig, ax = plt.subplots()
    ax.hist(tau, weights=counts, bins=30)
    for c in ax.containers:
        ax.bar_label(c, labels=[v if v > 0 else '' for v in c.datavalues])
    ax.set_title('Amount of triple vectors with aggregate variance at most tau')
    ax.set_xlabel('Tau')
    ax.set_ylabel('Counts')
    plt.show()
