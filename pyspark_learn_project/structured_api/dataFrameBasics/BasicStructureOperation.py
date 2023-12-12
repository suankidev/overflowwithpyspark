import pyspark.sql
from common_utils.sparkUtils import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, Row, DecimalType, DoubleType
from pyspark.sql.functions import col, column, expr, count, avg, sum, countDistinct, lit


class BasicStructureOperaion:

    def __init__(self, df: pyspark.sql.dataframe, sch: pyspark.sql.types.StructType, spark: pyspark.sql.SparkSession):
        self.df = df
        self.sch = sch
        self.spark = spark

    def df_basics(self, flightDf2015):
        source = col("source")
        print(type(source))

    def df_manual_creation(self, spark: pyspark.sql.SparkSession):
        test = Row(1, "hello", False, 4, 567.34973)
        print(test[0])

        myschema = StructType([StructField("name", StringType(), False), StructField("Age", StringType(), False)])
        names = [Row("sujeet", "30"), Row("Ankita", "24")]
        spark.createDataFrame(names, myschema).show()
        spark.createDataFrame(names, ["name", "age"]).show()
        spark.sparkContext.parallelize(names).toDF(["name", "age"]).show()

    def df_expression(self, flightDF, spark: pyspark.sql.SparkSession):
        '''set a Boolean flag for when the origin country is the same as the destination country'''
        flightDF.select(col('*'), (col("source") == col("target")).alias("same")) \
            .where(col("same") == True)  # .show()

        # flightDF.select(expr("source as origin"),col("target").alias("destination")).show()

        # flightDF.select(avg(col("totalflight")), countDistinct(col("source")))  # .show()

        '''converting to spark type literal: literal are expression'''

        flightDF.select(col('*'), lit('1').alias('one'))  # .show(5)

        '''renaming the column'''
        flightDF.withColumnRenamed('target', 'destination')  # .show()

        '''case sensitivity'''
        # spark.conf.set('spark.sql.caseSensitive', 'true')

        '''removing column'''

        flightDF.drop('target').columns

        '''casting column'''
        flightDF.select(col('*'),
                        col('totalflight').cast(DecimalType(14, 5)).alias('testcol'))  # .show(5, truncate=False)

        testdata = [Row('A', 3637.8885), Row('B', 34384.3432), Row('C', 34386.555)]
        testschema = StructType([StructField('NAME', StringType(), False), StructField('VALUE', DoubleType(), False)])

        testdf = spark.createDataFrame(testdata, testschema)

        testdf.show()

    def filtering_row(self, flightdf):
        rangeDf = self.spark.range(100)
        rangeDf.toDF('number')
        # rangeDf.show(truncate=False)

        # target source totalflight
        print(flightdf.count())
        # flightdf.where(col("totalflight") < 2).show()

        print(flightdf.select('source', 'target').distinct().count())

        # random sample

        seed = 5
        withReplacement = False
        fraction = 0.5
        flightdf.sample(withReplacement, fraction, seed).show()

    def operation(self, df:pyspark.sql.DataFrame):
        """concatinating and appending row
"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        spark = self.spark
        dfschema = StructType([StructField('origin', StringType(), False),
                               StructField('destination', StringType(), False),
                               StructField('total', IntegerType(), True)])

        data_one = [Row('India', 'Bhutan', 3), Row('India', 'Canada', 4)]
        data_two = [Row('Canada', 'India', 5), Row('Bhutan', 'Nepal', 3), Row('India', 'Canada', 6)]

        df_one = spark.createDataFrame(data_one, dfschema)
        df_two = spark.createDataFrame(data_two, dfschema)

        # df_one.show()
        # df_two.show()

        uniondf = df_one.union(df_two)
        '''+------+-----------+-----+
            |origin|destination|total|
            +------+-----------+-----+
            | India|     Bhutan|    3|
            | India|     Canada|    4|
            |Canada|      India|    5|
            |Bhutan|      Nepal|    3|
            | India|     Canada|    6|
            +------+-----------+-----+'''

        # uniondf.groupby('origin', 'destination').agg(sum('total')).show()

        '''sorting rows'''

        uniondf.sort('origin', ascending=False)

        df.sort('totalflight').show(5, truncate=False)
        df.orderBy('source', 'target').show(5, truncate=False)
        df.orderBy(col('totalflight').desc()).show(5, truncate=False)
        df.sort(col('source').desc(), col('target').desc()).limit(5).show()

    def repartion_and_coalesce(self, df):
        """Another important optimization opportunity is to partition the data according to some frequently
filtered columns, which control the physical layout of data across the cluster including the
partitioning scheme and the number of partitions"""

        '''
        you should typically only repartition when the future number of partitions is greater
than your current number of partitions or when you are looking to partition by a set of columns'''

        print(df.rdd.getNumPartitions())

        df = df.repartition(5)

        print(df.rdd.getNumPartitions())
        # or if you know the frequently used column
        # df = df.repartition(col('source'))

        collectdf = df.limit(10)
        print(collectdf.take(5))  # take work with interger count
        collectdf.show()
        collectdf.collect()

        # The method toLocalIterator collects partitions to the driver as an iterator
        test = collectdf.toLocalIterator()

        for i, j in enumerate(test):
            print(i, j, j[0])


def basic_structure_operation():
    spark = get_spark_session()
    file_path = "C:\\Users\\sujee\\Desktop\\spark_input\\fligt_data\\2015*.csv"
    flight_schema = StructType([StructField("target", StringType(), False),
                                StructField("source", StringType(), False),
                                StructField("totalflight", StringType(), False, metadata={"hello": "world"})]
                               )
    flightData2015 = spark.read.option("header", "true").schema(flight_schema).format("csv").load(file_path) \
        .sortWithinPartitions(col('totalflight'))
    myobj = BasicStructureOperaion(flightData2015, flight_schema, spark)

    myobj.repartion_and_coalesce(flightData2015)
    # myobj.filtering_row(flightData2015)
    # myobj.operation(flightData2015)

    # myobj.df_basics(flightData2015)
    # myobj.df_manual_creation(spark)
#    myobj.df_expression(flightData2015, spark)
