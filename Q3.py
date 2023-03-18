from pyspark import SparkContext, RDD

def q3_sol(spark_context: SparkContext, rdd: RDD):    
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