import pyspark.sql
from pyspark.sql.types import DoubleType

from common_utils.sparkUtils import get_flight_df, get_spark_session, get_retail_data
from pyspark.sql.functions import lit, col, instr, round, bround, initcap, lower, upper, desc, current_date, \
    current_timestamp, date_sub, date_add, datediff, to_date, months_between, from_unixtime, unix_timestamp, \
    to_timestamp, coalesce, struct, collect_set, collect_list, split, size, array_contains, explode, create_map, udf
from pyspark.sql.functions import regexp_extract, regexp_replace, translate, locate, expr

'''
Booleans         --> make sure value is not null eeNullsagfe()
Numbers
Strings
Dates and timestamps
Handling null
Complex types
User-defined functions
'''


class WorkingWithDifferentDataType:
    retail_df = get_retail_data()

    @staticmethod
    def one():
        flight_df = get_flight_df()
        retail_df = get_retail_data()
        spark = get_spark_session()

        # retail_df.printSchema()
        '''root
 |-- InvoiceNo: string (nullable = true)
 |-- StockCode: string (nullable = true)
 |-- Description: string (nullable = true)
 |-- Quantity: integer (nullable = true)
 |-- InvoiceDate: string (nullable = true)
 |-- UnitPrice: double (nullable = true)
 |-- CustomerID: double (nullable = true)
 |-- Country: string (nullable = true)
 +---------+---------+-----------------------------------+--------+-------------------+---------+----------+--------------+
|InvoiceNo|StockCode|Description                        |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country       |
+---------+---------+-----------------------------------+--------+-------------------+---------+----------+--------------+
|536365   |85123A   |WHITE HANGING HEART T-LIGHT HOLDER |6       |2010-12-01 08:26:00|2.55     |17850.0   |United Kingdom|
|536365   |71053    |WHITE METAL LANTERN                |6       |2010-12-01 08:26:00|3.39     |17850.0   |United Kingdom|
|536365   |84406B   |CREAM CUPID HEARTS COAT HANGER     |8       |2010-12-01 08:26:00|2.75     |17850.0   |United Kingdom|
|536365   |84029G   |KNITTED UNION FLAG HOT WATER BOTTLE|6       |2010-12-01 08:26:00|3.39     |17850.0   |United Kingdom|
|536365   |84029E   |RED WOOLLY HOTTIE WHITE HEART.     |6       |2010-12-01 08:26:00|3.39     |17850.0   |United Kingdom|
+---------+---------+-----------------------------------+--------+-------------------+---------+----------+--------------+
only showing top 5 rows
 '''

        # retail_df.show(5, False)
        '''Converting to Spark Types'''
        '''The lit function. This function converts a
type in another language to its correspnding Spark representation.'''

        df = retail_df.select(lit(5), lit('five'), lit(5.0))
        # df.show()

        '''Working with Booleans'''

        df = retail_df.where(col('InvoiceNo') == 536365).select(col('InvoiceNo'), col('Description'))

        price_filter = col('UnitPrice') > 600
        descriptor_filter = instr(col('description'), 'POSTAGE') >= 1

        retail_df.where(col('StockCode').isin('DOT')).where(price_filter | descriptor_filter)

        df.show(5, False)

        dot_code_filter = col("StockCode") == 'DOT'

        retail_df.withColumn('isExpensive', dot_code_filter & (price_filter | descriptor_filter)) \
            .where(col('isExpensive')).show(5, False)

        stock_code = {'DOT', '71053', 'test'}

        retail_df.where(col('StockCode').isin(*stock_code)).show(5, False)

        retail_df.where(not (dot_code_filter & (price_filter | descriptor_filter))).select('description',
                                                                                           'unitprice').show()

    @staticmethod
    def working_with_number():
        fabricated_value = pow(col('quantity') * col('UnitPrice'), 2) + 5
        df = retail_df.select(col('customerid'), fabricated_value.alias('realQuantity'))

        df = retail_df.select(round(lit(2.5)), bround(lit(2.5)))
        df.show(5, False)

    @staticmethod
    def working_with_string():
        retail_df = WorkingWithDifferentDataType.retail_df

        '''initcap to capatalise '''

        df = retail_df.select(initcap(col('Description')), lower(col('description')), upper(col('description')))
        df.show(2, False)

        from pyspark.sql.functions import ltrim, rtrim, trim, rpad, lpad

        '''Note that if lpad or rpad takes a number less than the length of the string, 
        it will always remove 
        values from the right side of the string'''

        df = retail_df.select(ltrim(lit('    hello    ')), rpad(lit('Hello'), 8, '--'))
        df.show(2, False)

    @staticmethod
    def working_with_regular_expression():
        retail_df = WorkingWithDifferentDataType.retail_df

        regex_string = 'BLACK|WHITE|RED|GREEN|BLUE'
        extract_string = '(BLACK|WHITE|RED|GREEN|BLUE)'
        extract_string_one = '(^WHITE)'

        # find regex_string  in description column and replace it with 'COLOR' value
        df = retail_df.select(regexp_replace(col('Description'), regex_string, 'COLOR').alias('color_clean'),
                              col('description'))

        df.show(5, False)

        # replace given characters with other characters

        df = retail_df.select(translate(col('description'), 'LEET', '1337'))

        df.show(5, False)

        # pulling out first mention color
        '''Extract a specific group matched by a Java regex, from the specified string column. 
        If the regex did not match, or the specified group did not match, an empty string is returned'''

        df = retail_df.select(regexp_extract(col('description'), extract_string, 1).alias('extract_column'),
                              col('description'))

        df.show(5, False)

        df = retail_df.select(regexp_extract(col('description'), extract_string_one, 1).alias('extract_column'),
                              col('description'))

        df.show(5, False)

        '''check if the description contain the value BLACK or WHITE'''

        filter_black = instr(col('description'), 'BLACK') >= 1
        filter_white = instr(col('description'), 'RED') >= 1

        df = retail_df.withColumn('has_simple_color', (filter_black | filter_white)).where('has_simple_color') \
            .select('description')

        df.show(5, False)

        '''what if we have multiple value to find in a column , if exist or not'''

        simple_color = ['black', 'red', 'white', 'green', 'blue']

        def color_locator(column, color_string):
            return locate(color_string.upper(), column) \
                .cast("boolean") \
                .alias("is_" + color_string)

        selected_columns = [color_locator(retail_df.Description, c) for c in simple_color]
        selected_columns.append(expr("*"))  # has to a be Column type
        df = retail_df.select(*selected_columns).where(expr("is_white OR is_red")) \
            .select("Description")

        df.show(3, False)

        df = retail_df.select(locate('RED', col('description')).alias('description_located'), col('description'))
        df.show(5, False)

    @staticmethod
    def working_with_date_time():
        retail_df = get_retail_data(get_spark_session())
        flight_df = get_flight_df(get_spark_session())
        spark = get_spark_session()
        date_df = spark.range(10) \
            .withColumn("today", current_date()) \
            .withColumn("now", current_timestamp())

        date_df.printSchema()
        # date_df.show(truncate=False)

        date_df.select(date_sub(col("today"), 5), date_add(col("today"), 5))

        date_df.withColumn("week_ago", date_sub(col("today"), 7)) \
            .select(datediff(col("week_ago"), col("today")))

        date_df.select(
            to_date(lit("2016-01-01")).alias("start"),
            to_date(lit("2017-05-22")).alias("end")
        ).select(col('start'), col('end'), months_between(col("start"), col("end"))).show(1)

        """Unix time is also known as Epoch time which specifies the moment in time since 1970-01-01 00:00:00 UTC
        unix_timestamp() – Converts Date and Timestamp Column to Unix
        https://sparkbyexamples.com/pyspark/pyspark-sql-working-with-unix-time-timestamp/#:~:text=In%20PySpark%20SQL%2C%20unix_timestamp%20%28%29%20is%20used%20to,UTC%29%20to%20a%20string%20representation%20of%20the%20timestamp.
        
        yyyy-mm-dd
        """
        inputData = [("2019-07-01 12:01:19",
                      "07-01-2019 12:01:19",
                      "07-01-2019")]
        columns = ["timestamp_1", "timestamp_2", "timestamp_3"]
        df = spark.createDataFrame(
            data=inputData,
            schema=columns)
        # df.printSchema()
        # df.show(truncate=False)

        """convert timestamp to unix timestamp"""
        df2 = df.select(
            unix_timestamp(col("timestamp_1")).alias("timestamp_1"),
            unix_timestamp(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
            unix_timestamp(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
            unix_timestamp().alias("timestamp_4")
        )
        df2.printSchema()
        df2.show(truncate=False)

        # Convert; Unix; timestamp; to; timestamp

        df3 = df2.select(
            from_unixtime(col("timestamp_1")).alias("timestamp_1"),
            from_unixtime(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
            from_unixtime(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
            from_unixtime(col("timestamp_4")).alias("timestamp_4")
        )
        # df3.printSchema()
        # df3.show(truncate=False)

        # Spark will not throw an error if it cannot parse the date; rather, it will just return null

        dateFormat = "yyyy-dd-MM"
        cleanDateDF = spark.range(1).select(
            to_date(lit("2017-12-11"), dateFormat).alias("date"),
            to_date(lit("2017-20-12"), dateFormat).alias("date2"))
        cleanDateDF.show(5, False)

        cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
        cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
        """After we have our date or timestamp in the correct format and type, comparing between them is
actually quite easy. We just need to be sure to either use a date/timestamp type or specify our
string according to the right format of yyyy-MM-dd if we’re comparing a date"""

    @staticmethod
    def work_with_null_date():
        """always use nulls to represent missing or empty data in your
DataFrames. Spark can optimize working with null values more than it can if you use empty
strings or other values. """
        retail_df = get_retail_data(get_spark_session())
        flight_df = get_flight_df(get_spark_session())

        retail_df.select(coalesce(col('description'), col('customerid')))
        # .show(5, False)

        # ifnull, nullIf, nvl, and nvl2

        spark = get_spark_session()

        # drop function to drop value

        retail_df.na.drop()
        retail_df.na.drop("any")  # drop if any value is null in a row
        retail_df.na.drop("all")  # drop if all values in a row in null

        retail_df.na.drop("all", subset=['description', 'customerid'])

        # fill all value to a string if it's a null
        retail_df.na.fill("replace value")
        fill_cols_value = {"StockCode": 5, "Description": "No Value"}
        retail_df.na.fill(fill_cols_value)

        # replace value with other
        retail_df.na.replace([""], ["UNKNOWN"], "Description")

        # Ordering
        # asc_nulls_first, desc_nulls_first, asc_nulls_last, or desc_nulls_last t

    @staticmethod
    def working_with_complex_type():
        retail_df = get_retail_data(get_spark_session())
        flight_df = get_flight_df(get_spark_session())

        """There are three kinds of complex types: structs, arrays,
and maps."""
        retail_df.show(5, False)
        complex_df = retail_df.select(struct(col('description'), col('customerid')).alias('complex'), col('*'))
        complex_df.show(5, truncate=False)
        complex_df.select(col('complex').getField('customerid'))  # .show(5, False)
        complex_df.select(expr("complex.*"))
        # .show(5, False)
        """our objective is to take
every single word in our Description column and convert that into a row in our DataFrame"""

        complex_ar = retail_df.select(split(col('description'), " ").alias('splitcol'), col('description'),
                                      col('customerid'))

        complex_ar.select(expr("splitcol[0]"), size(col('splitcol'))).show(1)

        # array contain
        complex_ar.select(array_contains(col('splitcol'), 'WHITE').alias('arrycontain')).show(5, truncate=False)

        complex_ar.select(col("*"), explode(col('splitcol'))).show(10, False)

        retail_df.select(create_map(col('description'), col('invoiceno')).alias('complex_map')) \
            .select(col('complex_map')['WHITE METAL LANTERN']).show()

    @staticmethod
    def working_with_user_defined_function():
        def pow3(num):
            return num * num * num

        spark = get_spark_session()
        example = spark.range(10).toDF('num')
        udf3 = udf(pow3)
        example.select(udf3(col('num'))).show(5, False)

        spark.udf.register('power_3', pow3, DoubleType())
        example.createOrReplaceTempView('temp')
        spark.sql("select power_3(num) from temp").show()


def working_with_different_data_types():
    # WorkingWithDifferentDataType().one()
    # WorkingWithDifferentDataType.working_with_number()
    # WorkingWithDifferentDataType.working_with_string()
    # WorkingWithDifferentDataType.working_with_date_time()
    # WorkingWithDifferentDataType.work_with_null_date()
    # WorkingWithDifferentDataType.working_with_complex_type()
    WorkingWithDifferentDataType.working_with_user_defined_function()
