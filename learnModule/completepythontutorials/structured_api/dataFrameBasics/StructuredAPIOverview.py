"""
Datasets
DataFrames
SQL tables and views
"""
from common_utils.sparkUtils import get_spark_session
import pyspark.sql
from pyspark.sql.types import *
import datetime
from pyspark.sql.types import Row

"""
A schema defines the column names and types of a DataFrame

Schema manually --> often called schema on read
)

-->
Columns represent a simple type like an integer or string, a complex type like an array or map, or
a null value

-->
Rows
A row is nothing more than a record of data. Each record in a DataFrame must be of type Row
"""


class Chapter4:
    def chapter4_a(self, spark: pyspark.sql.SparkSession):
        numdf = spark.range(10).toDF("num")
        numdf.select(numdf['num'] + 10).show()

    def sparkTypes(self, spark: pyspark.sql.SparkSession):
        b = ByteType()  # -127 to +128
        s = ShortType()  # -32768 to +32768
        i = IntegerType()  #
        l = LongType()  # 9223372036854775807 to 9223372036854775808   8byte signed
        # DecimalType; StringType; BooleanType; BinaryType; TimestampType; DateType; ArrayType; MapType; StructType; StructField
        print(type(b))

        # allTypes = spark.sparkContext.parallelize([Row(i=1, s="string", d=1.0, l=1,b = True, list = [1, 2, 3], dict = {"s": 0}, row = Row(a=1),time = datetime(2014, 8, 1, 14, 1, 5))])
        # print(allTypes.take(1))

        def apiExecution(self):
            """Write DataFrame/Dataset/SQL Code.
    If valid code, Spark converts this to a Logical Plan.
    Spark transforms this Logical Plan to a Physical Plan, checking for optimizatio
    the way.
    Spark then executes this Physical Plan (RDD manipulations) on the cluster.
    """
            pass;

        # End of class


def chapter_call():
    myobj = Chapter4()
    myobj.sparkTypes(get_spark_session())
