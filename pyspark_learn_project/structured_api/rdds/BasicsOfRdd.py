from common_utils.sparkUtils import get_spark_session


class BasicOfRdd:
    """
    RDD: resilient distributed dataset low level api of spark
    when to use?
    1. tight control over data (/b/c of object of row)
    2. custom distributed variable
    3. to maintain some legacy code base
     RDD represents an immutable, partitioned collection of records that can be operated
on in parallel. Unlike DataFrames though, where each record is a structured row containing
fields with a known schema, in RDDs the records are just Java, Scala, or Python objects of the
programmer’s choosing
    """

    def demo(self):
        spark = get_spark_session()
        num_rdd = spark.range(500).rdd
        num_rdd1 = num_rdd.map(lambda x: x[0])
        print(num_rdd.take(5))

        # From local collection
        myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
        coll_rdd = spark.sparkContext.parallelize(myCollection, 2)
        print(coll_rdd.count())
        print(coll_rdd.collect())
        coll_rdd = coll_rdd.map(lambda x: x)
        print(coll_rdd.collect())

        print(coll_rdd.distinct().count())

    def filter_rdd(self):
        spark = get_spark_session()
        my_collection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
        coll_rdd = spark.sparkContext.parallelize(my_collection, 2)

        def start_wit(individual):
            return individual.startswith("S")

        filtered_coll_rdd = coll_rdd.filter(lambda x: start_wit(x))
        print(filtered_coll_rdd.collect())

        map_coll_rdd = coll_rdd.map(lambda x: (x, x[0], start_wit(x)))
        print(map_coll_rdd.collect())

        # flattern the output of map with char by char
        flat_map_coll_rdd = coll_rdd.flatMap(lambda x: x)
        print(flat_map_coll_rdd.collect())

        sorted_coll_rdd = coll_rdd.sortBy(lambda x: len(x), ascending=True)
        print(sorted_coll_rdd.collect())

        # Action

        def reducer(left, right):
            if len(left) > len(right):
                return left
            else:
                return right

        word_reduced_rdd = coll_rdd.reduce(lambda x, y: reducer(x, y))
        print(word_reduced_rdd)

        word = spark.range(1, 34).rdd
        print(word.first())
        print(word.min())
        print(word.max())

        word.saveAsTextFile("C:\\Users\\sujee\\pydev\\pyspark_learn_project\\resources\\words_rdd")
        """To set a compression codec, we must import the proper codec from Hadoop. You can find these
in the org.apache.hadoop.io.compress library:"""

    def caching_and_storage_level(self):
        spark = get_spark_session()
        my_collection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
        coll_rdd = spark.sparkContext.parallelize(my_collection, 2)

        print(f"checkpoint: {coll_rdd.getCheckpointFile()},"
              f" storage_level:{coll_rdd.getStorageLevel()},"
              f"num_of_partition:{coll_rdd.getNumPartitions()}")
        """org.apache.spark.storage.StorageLevel"""
        coll_rdd.cache()
        spark.sparkContext.setCheckpointDir("C:\\Users\\sujee\\pydev\\pyspark_learn_project\\resources\\checkping_dir")
        coll_rdd = coll_rdd.repartition(4)
        coll_rdd.checkpoint()

        print(f"checkpoint: {coll_rdd.getCheckpointFile()},"
              f" storage_level:{coll_rdd.getStorageLevel()},"
              f"num_of_partition:{coll_rdd.getNumPartitions()}")

    def key_value_based_rdd(self):
        spark = get_spark_session()
        my_coll = "Spark The Definitive Guide : Big Data Processing Made Simple" \
            .split(" ")
        words = spark.sparkContext.parallelize(my_coll, 2)

        pair_rdd_word = words.map(lambda x: (x.lower(), 1))
        words_key_by = words.keyBy(lambda x: x.lower()[0])

        print(words_key_by.collect())
        print(pair_rdd_word.collect())

        words_key_by = words_key_by.mapValues(lambda x: x.upper())

        print(words_key_by.collect())

        flat_map_value = words_key_by.flatMapValues(lambda x: x.upper())
        print(flat_map_value.collect())

        print(words_key_by.keys().collect())
        print(words_key_by.values().collect())

        print(words_key_by.lookup('S'))

    def rdd_operation_on_file(self):
        spark = get_spark_session()
        flight_path = "../../resources/spark_input/flight_data/2015-summary.csv"
        readme_path = "../../resources/spark_input/README.txt"
        flight_path_rdd = spark.sparkContext.textFile(flight_path)
        readme_path_rdd = spark.sparkContext.textFile(readme_path)
        header = flight_path_rdd.first()

        # print(flight_path_rdd.collect())
        flight_path_rdd_map = flight_path_rdd.filter(lambda x: x != header).map(lambda row: row.split(","))
        # print(flight_path_rdd_map.take(5))
        # print(flight_path_rdd_map.map(lambda x: x[0]).collect())

        counts_rdd = flight_path_rdd_map.filter(lambda x: x[0] == 'United States')
        # print(counts_rdd.collect())

        # print(readme_path_rdd.collect())

        splitted = readme_path_rdd.flatMap(lambda w: w)
        # print(splitted.take(10))
        print(flight_path_rdd_map.collect())
        print(flight_path_rdd_map.groupBy(lambda x: x[0]).map(lambda x: (x[0], x[1].map(lambda x: x[2]))).take(5))

    def test(self):
        spark = get_spark_session()
        path = "C:\\Users\\sujee\\pydev\\pyspark_learn_project\\resources\\mystartups.csv"

        startup_rdd = spark.sparkContext.textFile(path)
        header = startup_rdd.first()
        startup_rdd = startup_rdd.filter(lambda row: row != header).map(lambda x: x.split(","))
        print(startup_rdd.take(4))
        val = startup_rdd.map(lambda x: float(x[0])).sum()
        print(val)
        print(
            startup_rdd.map(lambda x: groupByKey(lambda x: x[3]).take(5)
                            )


def basic_of_rdd():
    obj = BasicOfRdd()
    # obj.key_value_based_rdd()
    # obj.rdd_operation_on_file()
    #obj.test()


if __name__ == '__main__':
    basic_of_rdd()
