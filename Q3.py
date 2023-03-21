from pyspark import SparkContext, RDD
import numpy as np

def q3_sol(spark_context: SparkContext, rdd: RDD): 

    def agg_var(agg_vec):

        np_vec = np.array(agg_vec)
        sq_vec = np.square(np_vec)

        som = np.sum(np_vec)
        som_sq = np.sum(sq_vec)

        length_vector = len(agg_vec)
        avg = 1 / length_vector * som
        avg_sq = avg*avg
        sq_som = 1 / length_vector * som_sq
        
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
    rdd = rdd.repartition(5).flatMap(lambda line: agg_vec1(line, vs, vi))\
                            .flatMap(lambda line: agg_vec2(line, vs, vi))\
                            .map(lambda line: agg_var(line[1])).collect() #only need the variance, then count how many above 410

    numpy_rdd = np.array(rdd)
    below_410 = (numpy_rdd < 410)
    below_20 = (numpy_rdd < 20)
    print(f">>COUNT {below_410.sum()}")
    print(f">>COUNT {below_20.sum()}")
        

    return