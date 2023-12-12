from common_utils.sparkUtils import get_spark_session

spark = get_spark_session()

# defining broadcast variable

"""Broadcast variables are shared, immutable variables
that are cached on every machine in the cluster instead of serialized with every single task"""

my_collection = "Spark The Definitive Guide : Big Data Processing Made Simple" \
    .split(" ")
words = spark.sparkContext.parallelize(my_collection, 2)

supplementalData = {"Spark": 1000, "Definitive": 200,
                    "Big": -300, "Simple": 100}

suppBroadcast = spark.sparkContext.broadcast(supplementalData)

# suppBroadcast.value

w = words.map(lambda x: (x, suppBroadcast.value.get(x, 0))).sortBy(lambda x: x[1])

print(w.collect())

"""Accumulator: Spark’s second type of shared variable, are a way of updating a
value inside of a variety of transformations and propagating that value to the driver node in an
efficient and fault-tolerant way."""

flight_path = "../resources/spark_input/flight_data/2015-summary.csv"
flight_path_rdd = spark.read.option('header', True).option('inferSchema', True).csv(flight_path)

"""let’s create an accumulator that will count the number of flights to or from China"""
accChina = spark.sparkContext.accumulator(0)


def accChinaFunc(flight_row):
    destination = flight_row["DEST_COUNTRY_NAME"]
    origin = flight_row["ORIGIN_COUNTRY_NAME"]
    if destination == "China":
        accChina.add(flight_row["count"])
    if origin == "China":
        accChina.add(flight_row["count"])


flight_path_rdd.foreach(lambda flight_row: accChinaFunc(flight_row))

print(accChina.value)
