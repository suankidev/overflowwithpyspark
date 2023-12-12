from common_utils.sparkUtils import get_flight_df, get_retail_data
from common_utils.sparkUtils import get_spark_session
from pyspark.sql.functions import count, first, last, count_distinct, \
    sum, sumDistinct, col, min, max, avg, collect_list, collect_set, expr \
    , row_number, rank, dense_rank, to_date

from pyspark.sql.window import Window


class WorkingWithAggregate:

    def __init__(self):
        self.retail_df = get_retail_data(get_spark_session())
        self.flight_df = get_flight_df(get_spark_session())
        self.rdf = self.retail_df.coalesce(5).cache()
        self.rdf.createOrReplaceTempView('retail_df')

    def working_with_aggregate_function(self):
        rdf = self.retail_df.coalesce(5)
        rdf.cache()  # MEMORY_AND_DISK
        rdf.createOrReplaceTempView('retail_df')

        print(rdf.count())

        rdf.select(count('description')).show()  # will not count null
        rdf.select(count('*')).show()  # will count along with null
        try:
            rdf.select(count_distinct('stockcode')).show()
            rdf.select(sum('quantity'), sumDistinct('quantity')).show()
        except Exception as e:
            print(e)
        rdf.select(first('stockcode'), last('stockcode')).show()
        rdf.select(max('quantity'), min('quantity')).show()

        rdf.select(
            count('quantity'),
            avg('quantity'),
            sum('quantity')
        ).show()

        # Aggregating to complex type

        rdf.agg(collect_list('country'), collect_set('country')).show()

    def working_with_group_data(self):
        """Thus far, we have performed only DataFrame-level aggregations. A more common task is to
perform calculations based on groups in the data. which we group our data on one column and perform some calculations on the other columns
that end up in that group"""
        rdf = self.retail_df.coalesce(5)
        rdf.cache()  # MEMORY_AND_DISK
        rdf.createOrReplaceTempView('retail_df')

        rdfg = rdf.groupby('invoiceno', 'customerid')
        rdfg.count().show(5, False)

        rdfg.agg(count('Quantity').alias('test'),
                 expr('count(Quantity)').alias('test1')).show()

    def working_with_window_function(self):
        """Window functions operate on a group of rows
        (like frame, partition) and return a single value for every input row. """
        """it support
        ranking function --> row_number(), rank(), dense_rank(), percent_rank(), 
        analytic function
        aggregate function
        """
        simple_data = [("James", "Sales", 3000),
                       ("Michael", "Sales", 4600),
                       ("Robert", "Sales", 4100),
                       ("Maria", "Finance", 3000),
                       ("James", "Sales", 3000),
                       ("Scott", "Finance", 3300),
                       ("Jen", "Finance", 3900),
                       ("Jeff", "Marketing", 3000),
                       ("Kumar", "Marketing", 2000),
                       ("Saif", "Sales", 4100)
                       ]

        columns = ["employee_name", "department", "salary"]

        spark = get_spark_session()
        df = spark.createDataFrame(simple_data, columns)

        # df.show(5, False)

        # row_numer --> sequential row number starting from 1 for each window partition

        window_spec = Window.partitionBy('department').orderBy(col('salary').desc())

        df.withColumn('salrank', row_number().over(window_spec)).show(10, False)
        df.withColumn('salrank', rank().over(window_spec)).show(10, False)
        df.withColumn('salrank', dense_rank().over(window_spec)).show(10, False)

        """how to calculate sum, min, max for each department using PySpark SQL
         Aggregate window functions and WindowSpec.
         When working with Aggregate functions, 
         we don’t need to use order by clause."""
        window_spec = Window.partitionBy('department').orderBy(col('salary').desc())
        window_spec_agg = Window.partitionBy('department')

        df.withColumn('salrank', rank().over(window_spec)) \
            .withColumn('avg', avg(col('salary')).over(window_spec_agg)) \
            .withColumn('sum', sum(col('salary')).over(window_spec_agg)) \
            .where(col('salrank') == 1) \
            .show(5, False)

        df_with_date = self.rdf.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
        window_spec = Window.orderBy(col('salary').desc())
        df.withColumn('salrank', rank().over(window_spec)).show(5, False)


def working_with_aggregate():
    obj = WorkingWithAggregate()
    obj.working_with_window_function()
