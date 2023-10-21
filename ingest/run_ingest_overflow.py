import pyspark.sql
from pyspark.sql.functions import col, locate, expr, sum
from pyspark.sql.functions import *

from flowutils.commonutils.SparkUtils import get_jdbc_df


def ingest_data(spark: pyspark.sql.SparkSession):
    print("running ingest workflow!")
    df = spark.range(50).toDF("num").where(col("num") % 2 == 0)
    df.show(5, truncate=False)



def get_df(spark:pyspark.sql.SparkSession):
    metadata = {'table': 'customers'}
    customerdf: pyspark.sql.DataFrame = get_jdbc_df(metadata, spark)
    productsdf: pyspark.sql.DataFrame = get_jdbc_df({'table': 'products'}, spark)

    retailer_path = r"C:\Users\sujee\Desktop\spark_input\retail-data\2010-12-01.csv"
    retailer_path_all = r"C:\Users\sujee\Desktop\spark_input\retail-data\*.csv"

    retailer: pyspark.sql.DataFrame = spark.read.format('csv').option('header', True).option('inferSchema', True) \
        .load(retailer_path)

    #retailer.show(3, truncate=False)
    retailer_schema = 'InvoiceNo int, StockCode string, description string,quantity int,invoicedata timestamp,unitprice float,customerid string,country string'
    retailer_with_schema:pyspark.sql.DataFrame = spark.read.format('csv').option('header',True).schema(retailer_schema)\
    .load(retailer_path)

    dict_data =  {'customerdf':customerdf,'retaildfwithoutschema':retailer,'productdf':productsdf,'retaildfwithschema':retailer_with_schema}


    all_retailer:pyspark.sql.DataFrame = spark.read.format('csv').option('header', True).option('inferSchema', True) \
        .load(retailer_path_all)

    dict_data.update({'all_retailer':all_retailer})
    return dict_data
def color_locator(column, color_string):
    return locate(color_string.upper(), column) \
        .cast("boolean") \
        .alias("is_" + color_string)

def summing_up(column, col_name):
    return sum(column).alias('sum_'+col_name)


def working_with_date_time(spark:pyspark.sql.SparkSession):
    ############working with date and time ############################3

    from pyspark.sql.functions import current_date, current_timestamp, date_sub, date_add, datediff, months_between, \
        date_trunc, date_format, datediff, to_date, lit, to_timestamp
    from pyspark.sql.functions import from_unixtime, unix_timestamp
    time_df = spark.range(5).withColumn('today', current_date()) \
        .withColumn('now', current_timestamp())

    time_df.printSchema()

    time_df.show(truncate=False)

    time_df.withColumn('date_sub', date_sub('today', 7)).withColumn('date_add', date_add('today', 7)) \
        .withColumn('lit_date_wron_formate', lit('12-22-30')).withColumn('to_date',
                                                                         to_date('lit_date_wron_formate', 'mm-yy-dd')) \
        .withColumn('start', to_date(lit('2022-01-01'))).withColumn('end', to_date(lit('2022-12-30'))) \
        .withColumn('to_timestamp', to_timestamp('start', 'yyyy-dd-mm')) \
        .withColumn('date_format', date_format('start', 'MM/dd/yyy')) \
        .show(truncate=False)

    inputData = [("2019-07-01 12:01:19",
                  "07-01-2019 12:01:19",
                  "07-01-2019")]
    columns = ["timestamp_1", "timestamp_2", "timestamp_3"]
    df = spark.createDataFrame(
        data=inputData,
        schema=columns)
    df.printSchema()
    df.show(truncate=False)
    df2 = df.select(
        unix_timestamp(col("timestamp_1")).alias("timestamp_1"),
        unix_timestamp(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
        unix_timestamp(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
        unix_timestamp().alias("timestamp_4")
    )
    df2.printSchema()
    df2.show(truncate=False)

    df3 = df2.select(
        from_unixtime(col("timestamp_1")).alias("timestamp_1"),
        from_unixtime(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
        from_unixtime(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
        from_unixtime(col("timestamp_4")).alias("timestamp_4")
    )
    df3.printSchema()
    df3.show(truncate=False)

def select_count_on_multiple(spark:pyspark.sql.SparkSession):
    #########select count on multiple columns#########

    users = [{"name":"Sujeet","salary":32000,'external_income':2300},
             {"name":"Kamesh","salary":1200,'external_income':7896},
             {"name":"Suresh","salary":7845,'external_income':8756},
             {"name": "Suresh", "salary": 7845, 'external_income': 8756}
             ]

    userdf = spark.createDataFrame(users)

    userdf.show(truncate=False)
    userdf.select(sum(col('external_income'))).show()
    selectedCol = [ summing_up(col(c),c) for c in ['external_income','salary']]
    selectedCol = [lambda x:sum(col(c)).alias('sum_'+c) for c in ['external_income', 'salary']]
    userdf.select(*selectedCol).show()
    print(selectedCol)
    userdf.groupby('name').agg(*selectedCol).show()


def work_with_regular_expression(spark):

    retailer = get_df(spark).get('retaildfwithoutschema7')
    ########REgular expression  ########33

    regex_string = "BLACK|WHITE|RED|GREEN|BLUE"

    retailer.withColumn('color_clean',regexp_replace(col('description'),regex_string,'SUJEET_COLOR'))\
    .select('description','color_clean').show(truncate=False)

    retailer.withColumn('color_clean', regexp_extract(col('description'), regex_string, 0)) \
        .select('description', 'color_clean').show(truncate=False)

    retailer.withColumn('color_clean', translate(col('description'), 'LEET', '1921')) \
        .select('description', 'color_clean').show(truncate=False)

    retailer.withColumn('color_clean', regexp_extract(col('description'), "^BOX", 0)) \
        .select('description', 'color_clean').show(truncate=False)

    containBlack = instr(col("description"),'BLACK') >=1
    containwhite = instr(col("description"),'WHITE') >=1

    retailer.withColumn('hascolour',containBlack | containwhite).where('hascolour').show()


    # what if there are too many values to be searched

    list_of_values = ['BLACK', 'WHITE', 'GREEN', 'RED']

    #isin is like in in sql
    #contains  .witll search in string, but what if we want to search too many values
    #instr index start with 1,

    retailer.withColumn('has_list_value', col('description').contains('BABUSHKA')).show(truncate=False)

    simpleColors = ["black", "white", "red", "green", "blue"]

    retailer.withColumn('testlocate',locate('WHITE',col('description'))).select(*['description','testlocate']).show()

    selectedColumns = [color_locator(retailer.Description, c) for c in simpleColors]
    #[Column<'CAST(locate(BLACK, Description, 1) AS BOOLEAN) AS `is_black`'>, Column<'CAST(locate(WHITE, Description, 1) AS BOOLEAN) AS `is_white`'>, Column<'CAST(locate(RED, Description, 1) AS BOOLEAN) AS `is_red`'>, Column<'CAST(locate(GREEN, Description, 1) AS BOOLEAN) AS `is_green`'>, Column<'CAST(locate(BLUE, Description, 1) AS BOOLEAN) AS `is_blue`'>]

    print(selectedColumns)
    retailer.select(*selectedColumns).show(truncate=False)

    selectedColumns.append(expr("*"))  # has to a be Column type
    df = retailer.select(*selectedColumns)
    df.show(truncate=False)

    df.where(expr("is_white OR is_red")).select("Description").show(3, False)



def working_with_null_data(spark):
    #{'customerdf':customerdf,'retaildfwithoutschema':retailer,'productdf':productsdf,'retaildfwithschema':retailer_with_schema}
    retailer = get_df(spark).get('retaildfwithschema')
    # retailer.show(5,truncate=False)

    username = [{"name":"sujeet","address":"Gorakhpur","salary":32000,"passportNumber":None,"AdharId":45124578},
                {"name":"kamesh","address":"Pune","salary":4587,"passportNumber":"ABDFPS111B167","AdharId":794516786},
                {"name": "Siva", "address": "Andhra", "salary": 89765, "passportNumber": None,"AdharId": 7956466}

                ]
    userdf = spark.createDataFrame(username)

    userdf.show()
    """
    +---------+---------+------+--------------+------+
    |  AdharId|  address|  name|passportNumber|salary|
    +---------+---------+------+--------------+------+
    | 45124578|Gorakhpur|sujeet|          null| 32000|
    |794516786|     Pune|kamesh| ABDFPS111B167|  4587|
    |  7956466|   Andhra|  Siva|          null| 89765|
    +---------+---------+------+--------------+------+
    """
    #coalesce --> if col1 != null: return col1 else: col2
    userdf.select('*',coalesce('passportNumber','adharid'))
    """
    +---------+---------+------+--------------+------+---------------------------------+
    |AdharId  |address  |name  |passportNumber|salary|coalesce(passportNumber, adharid)|
    +---------+---------+------+--------------+------+---------------------------------+
    |45124578 |Gorakhpur|sujeet|null          |32000 |45124578                         |
    |794516786|Pune     |kamesh|ABDFPS111B167 |4587  |ABDFPS111B167                    |
    |7956466  |Andhra   |Siva  |null          |89765 |7956466                          |
    +---------+---------+------+--------------+------+---------------------------------+
    """

    """
    ifnull(col1,col2) -> if col1: return col2 else: col1
    nullif(col1,col2) -> if col1 == col2: retunr null else: col2
    nvl -> if col1: return col2 else; col1
    """

    #dropping

    userdf.na.drop("any") #drop row any value is null
    userdf.na.drop("all") #drop row if all value are null
    userdf.na.drop('all',subset=['adharid','passportnumber'])

    #fill with some value

    userdf.na.fill("null become this")
    userdf.na.fill('all null will become this',subset=['adharid','passportnumber'])

    fill_cols_value = {"adharid":"XXXXXXXXXXX","passportnumber":'XXXXXXXXXXX'}

    userdf_filled = userdf.na.fill(fill_cols_value)
    """
         +---------+---------+------+--------------+------+
        |  AdharId|  address|  name|passportNumber|salary|
        +---------+---------+------+--------------+------+
        | 45124578|Gorakhpur|sujeet|  XXXXXXXXXXXX| 32000|
        |794516786|     Pune|kamesh| ABDFPS111B167|  4587|
        |  7956466|   Andhra|  Siva|  XXXXXXXXXXXX| 89765|
        +---------+---------+------+--------------+------+
    """
    #replace other than null
    userdf_filled.na.replace('XXXXXXXXXXX','TEST REPLACE','passportnumber').show()

    """
      
        +---------+---------+------+--------------+------+
        |  AdharId|  address|  name|passportNumber|salary|
        +---------+---------+------+--------------+------+
        | 45124578|Gorakhpur|sujeet|  TEST REPLACE| 32000|
        |794516786|     Pune|kamesh| ABDFPS111B167|  4587|
        |  7956466|   Andhra|  Siva|  TEST REPLACE| 89765|
        +---------+---------+------+--------------+------+
    """
    userdf_filled.na.replace(['XXXXXXXXXXX','sujeet'],['replaced_XXXX','Aish'],subset=['name','passportnumber']).show()
    """
          +---------+---------+------+--------------+------+
        |  AdharId|  address|  name|passportNumber|salary|
        +---------+---------+------+--------------+------+
        | 45124578|Gorakhpur|  Aish| replaced_XXXX| 32000|
        |794516786|     Pune|kamesh| ABDFPS111B167|  4587|
        |  7956466|   Andhra|  Siva| replaced_XXXX| 89765|
        +---------+---------+------+--------------+------+
    """

def working_with_complex_type(spark):
    # {'customerdf':customerdf,'retaildfwithoutschema':retailer,'productdf':productsdf,'retaildfwithschema':retailer_with_schema}
    retailer = get_df(spark).get('retaildfwithschema')

    ######## struct , Array, Map ###### 3 types  ############

    complexdf = retailer.select(struct('description','invoiceno').alias('complex'))

    #complexdf.show(truncate=False)
    """
        ---------------------------------------------+
        |complex                                      |
        +---------------------------------------------+
        |{WHITE HANGING HEART T-LIGHT HOLDER, 536365} |
        |{WHITE METAL LANTERN, 536365}                |
    """

    complexdf.select(col('complex').getField('description'),
                     'complex.description',
                     'complex.*',
                     )
    """
              +-----------------------------------+-----------------------------------+-----------------------------------+---------+
                |complex.description                |description                        |description                        |invoiceno|
                +-----------------------------------+-----------------------------------+-----------------------------------+---------+
                |WHITE HANGING HEART T-LIGHT HOLDER |WHITE HANGING HEART T-LIGHT HOLDER |WHITE HANGING HEART T-LIGHT HOLDER |536365   |
                |WHITE METAL LANTERN                |WHITE METAL LANTERN                |WHITE METAL LANTERN                |536365   |
                |CREAM CUPID HEARTS COAT HANGER     |CREAM CUPID HEARTS COAT HANGER     |CREAM CUPID HEARTS COAT HANGER     |536365   |
                |KNITTED UNION FLAG HOT WATER BOTTLE|KNITTED UNION FLAG HOT WATER BOTTLE|KNITTED UNION FLAG HOT WATER BOTTLE|536365   |

    """


    #Array

    splitted_col = split(col('description')," ").alias('splitted_col')

    retailer.select('description', splitted_col,
                    splitted_col[0].alias('first_val'), size(splitted_col).alias('arr_size'),
                    array_contains(splitted_col,'WHITE').alias('if_white'),

    ).show(5,truncate=False)


    #explode
    exploaded_df = retailer.select('description',splitted_col,explode(splitted_col).alias('exploded_col'))
    """
       +-----------------------------------+------------------------------------------+-------+
        |description                        |splitted_col                              |col    |
        +-----------------------------------+------------------------------------------+-------+
        |WHITE HANGING HEART T-LIGHT HOLDER |[WHITE, HANGING, HEART, T-LIGHT, HOLDER]  |WHITE  |
        |WHITE HANGING HEART T-LIGHT HOLDER |[WHITE, HANGING, HEART, T-LIGHT, HOLDER]  |HANGING|
        |WHITE HANGING HEART T-LIGHT HOLDER |[WHITE, HANGING, HEART, T-LIGHT, HOLDER]  |HEART  |
        |WHITE HANGING HEART T-LIGHT HOLDER |[WHITE, HANGING, HEART, T-LIGHT, HOLDER]  |T-LIGHT|
        |WHITE HANGING HEART T-LIGHT HOLDER |[WHITE, HANGING, HEART, T-LIGHT, HOLDER]  |HOLDER |
        |WHITE METAL LANTERN                |[WHITE, METAL, LANTERN]                   |WHITE  |
        |WHITE METAL LANTERN                |[WHITE, METAL, LANTERN]                   |METAL  |
        |WHITE METAL LANTERN                |[WHITE, METAL, LANTERN]                   |LANTERN|
    """
    #put it back

    exploaded_df.groupby('description').agg(collect_list('exploded_col')).show(truncate=False)

    #map

    created_map = create_map('description','invoiceno')
    retailr_map = retailer.select('description','invoiceno',created_map)
    retailr_map.show(5,truncate=False)

    retailr_map.select(created_map.getItem('WHITE METAL LANTERN')).show()
    retailr_map.select('*',explode(created_map)).show(truncate=False)

    """
       +-----------------------------------+---------+-----------------------------------------------+-----------------------------------+------+
        |description                        |invoiceno|map(description, invoiceno)                    |key                                |value |
        +-----------------------------------+---------+-----------------------------------------------+-----------------------------------+------+
        |WHITE HANGING HEART T-LIGHT HOLDER |536365   |{WHITE HANGING HEART T-LIGHT HOLDER -> 536365} |WHITE HANGING HEART T-LIGHT HOLDER |536365|
        |WHITE METAL LANTERN                |536365   |{WHITE METAL LANTERN -> 536365}                |WHITE METAL LANTERN                |536365|
    """




#create udf and register with spark

def power_of_three(value):
    return value ** 3

def working_with_udf(spark):
    df = spark.range(500).toDF('num')
    power_of_three_udf = udf(power_of_three)

    df.select(col('num'),power_of_three_udf(col('num'))).show(5)

    from pyspark.sql.types import IntegerType
    #to use in sql
    spark.udf.register('power3_sql',power_of_three,IntegerType())
    df.createOrReplaceTempView('test')
    spark.sql("""select power3_sql(num), num from test""").show(5)


def working_with_aggregation(spark):
    # {'customerdf':customerdf,'retaildfwithoutschema':retailer,'productdf':productsdf,'retaildfwithschema':retailer_with_schema}
    retailer = get_df(spark).get('retaildfwithschema')
    retailer.cache()
    retailer.createOrReplaceTempView('retailertable')

    print(retailer.count()) #

    retailer.select(count('stockcode'))
    retailer.select(first('stockcode'),last('stockcode'),min('quantity'),max('quantity'),sum('quantity')\
                    ,
                    avg('quantity'))
    """
      +----------------+---------------+-------------+-------------+-------------+-----------------+
        |first(stockcode)|last(stockcode)|min(quantity)|max(quantity)|sum(quantity)|    avg(quantity)|
        +----------------+---------------+-------------+-------------+-------------+-----------------+
        |          85123A|          20755|          -24|          600|        26814|8.627413127413128|
        +----------------+---------------+-------------+-------------+-------------+-----------------+

    """


    #aggregating to complex types

    retailer.agg(collect_list('country'),collect_set('country'))#.show(truncate=False)


    ########### GROUPING  ############3

    retailer.groupby('invoiceno','quantity').count()

    retailer.groupby('invoiceno').agg(
        count('quantity'),
        round(avg('quantity'),2)
    ).show()



def working_with_window_function(spark):

    #ranking, analytic and aggregate
    retailer = get_df(spark).get('all_retailer')
    retailer.cache()
    retailer.createOrReplaceTempView('all_retailer_tab')
    retailer.show(5,truncate=False)
    """
      +---------+---------+------------------------------+--------+-------------------+---------+----------+--------------+
    |InvoiceNo|StockCode|Description                   |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country       |
    +---------+---------+------------------------------+--------+-------------------+---------+----------+--------------+
    |537226   |22811    |SET OF 6 T-LIGHTS CACTI       |6       |2010-12-06 08:34:00|2.95     |15987.0   |United Kingdom|
    |537226   |21713    |CITRONELLA CANDLE FLOWERPOT   |8       |2010-12-06 08:34:00|2.1      |15987.0   |United Kingdom|
    |537226   |22927    |GREEN GIANT GARDEN THERMOMETER|2       |2010-12-06 08:34:00|5.95     |15987.0   |United Kingdom|
    |537226   |20802    |SMALL GLASS SUNDAE DISH CLEAR |6       |2010-12-06 08:34:00|1.65     |15987.0   |United Kingdom|
    |537226   |22052    |VINTAGE CARAVAN GIFT WRAP     |25      |2010-12-06 08:34:00|0.42     |15987.0   |United Kingdom|
    +---------+---------+------------------------------+--------+-------------------+---------+----------+--------------+
    only showing top 5 rows
    
    """

    simpleData = (("James", "Sales", 3000), \
                  ("Michael", "Sales", 4600), \
                  ("Robert", "Sales", 4100), \
                  ("Maria", "Finance", 3000), \
                  ("James", "Sales", 3000), \
                  ("Scott", "Finance", 3300), \
                  ("Jen", "Finance", 3900), \
                  ("Jeff", "Marketing", 3000), \
                  ("Kumar", "Marketing", 2000), \
                  ("Saif", "Sales", 4100) \
                  )

    columns = ["employee_name", "department", "salary"]
    df = spark.createDataFrame(data=simpleData, schema=columns)
    df.printSchema()
    df.show(truncate=False)

    from pyspark.sql.window import Window
    windspec = Window.partitionBy('department').orderBy('salary')

    df.withColumn('row_number',row_number().over(windspec))\
                  .withColumn('rank',rank().over(windspec))\
                  .withColumn('dens_rank',dense_rank().over(windspec))\
    .withColumn('ntile',ntile(2).over(windspec))\
    .show(truncate=False)

    windowSpecAgg = Window.partitionBy("department")
    df.withColumn("row", row_number().over(windspec)) \
        .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
        .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
        .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
        .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
        .where(col("row") == 1).select("department", "avg", "sum", "min", "max") \
        .show()


   #using pivot

    retailer.groupby(to_date('InvoiceDate')).pivot('country').sum().show()



def working_with_join(spark):
    emp = [(1, "Smith", -1, "2018", "10", "M", 3000), \
           (2, "Rose", 1, "2010", "20", "M", 4000), \
           (3, "Williams", 1, "2010", "10", "M", 1000), \
           (4, "Jones", 2, "2005", "10", "F", 2000), \
           (5, "Brown", 2, "2010", "40", "", -1), \
           (6, "Brown", 2, "2010", "50", "", -1) \
           ]
    empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", \
                  "emp_dept_id", "gender", "salary"]

    empDF = spark.createDataFrame(data=emp, schema=empColumns)
    empDF.printSchema()
    empDF.show(truncate=False)

    dept = [("Finance", 10), \
            ("Marketing", 20), \
            ("Sales", 30), \
            ("IT", 40) \
            ]
    deptColumns = ["dept_name", "dept_id"]
    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    deptDF.printSchema()
    deptDF.show(truncate=False)

    """
    
       +------+--------+---------------+-----------+-----------+------+------+
        |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|
        +------+--------+---------------+-----------+-----------+------+------+
        |1     |Smith   |-1             |2018       |10         |M     |3000  |
        |2     |Rose    |1              |2010       |20         |M     |4000  |
        |3     |Williams|1              |2010       |10         |M     |1000  |
        |4     |Jones   |2              |2005       |10         |F     |2000  |
        |5     |Brown   |2              |2010       |40         |      |-1    |
        |6     |Brown   |2              |2010       |50         |      |-1    |
        +------+--------+---------------+-----------+-----------+------+------+

        +---------+-------+
        |dept_name|dept_id|
        +---------+-------+
        |Finance  |10     |
        |Marketing|20     |
        |Sales    |30     |
        |IT       |40     |
        +---------+-------+

    
    """
    #inner , key doesn't match drop the records in left and right
    empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'inner').show(truncate=False)

    #outer, where record not matched return null
    empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "outer") \
        .show(truncate=False)

    empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "left") \
        .show(truncate=False)

    empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "right") \
        .show(truncate=False)

   #return all rows from left table only which setisy the join cond
    empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftsemi") \
        .show(truncate=False)

    # return row from left table which not setisfy the join condition
    empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftanti") \
        .show(truncate=False)





def working_with_oracle_data(spark):
    jdbc_df = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin:suanki/testpass@//localhost:1521/PDBORCL") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("user", 'suanki') \
        .option('password', 'testpass') \
        .option('dbtable', 'products').load()

    retailer = get_df(spark).get('all_retailer')

    print(retailer.count())
    print(retailer.rdd.getNumPartitions())


    outpath = r"C:\Users\sujee\Desktop\spark_output\warehouse_dir"

    # retailer.repartition(5).write.option('mode','overwrite').save(outpath+r'\filout_one')
    # retailer.repartition(1).write.option('mode', 'overwrite').save(outpath+r'\fileout_two')
    # retailer.repartition(5).write.option('mode', 'overwrite').partitionBy('country').save(outpath+r'\fileout_three')

    spark.sql("create database test")
    spark.sql("use test")

    retailer.repartition(1).write.option('mode', 'overwrite').saveAsTable(name='retaildata_table',mode='overwrite')
    spark.sql("show tables in test")

def countOfWord(spark):

    df = spark.read.text(r"C:\Users\sujee\Desktop\spark_input\tstfile.csv")
    df.show()

    grp_df = df.withColumn('grp',explode(split(col('value')," ")))

    grp_df.groupby('grp').agg(count('grp')).show()


def test_oracle_date(spark: pyspark.sql.SparkSession):
    # working_with_date_time(spark)
    # working_with_date_time(spark)
    # work_with_regular_expression(spark)
    # working_with_null_data(spark)
    #working_with_complex_type(spark)
    #working_with_udf(spark)
    #working_with_aggregation(spark)
    #working_with_window_function(spark)
    #working_with_join(spark)
    # working_with_oracle_data(spark)
    countOfWord(spark)
