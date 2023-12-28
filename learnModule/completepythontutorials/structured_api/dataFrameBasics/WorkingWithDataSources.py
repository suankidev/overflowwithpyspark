from common_utils.sparkUtils import *


class WorkingWithDataSources:
    '''

    Read modes:
    permissive :Sets all fields to null when it encounters a corrupted record and places all corrupted records
    in a string column called _corrupt_record

    dropMalformed : Drops the row that contains malformed records
    failFast : Fails immediately upon encountering malformed records

    Save mode:
    append : Appends the output files to the list of files that already exist at that location
    overwrite :Will completely overwrite any data that already exists there
    errorIfExists: Throws an error and fails the write if data or files already exist at the specified location
    ignore : If data or files exist at the location, do nothing with the current DataFrame
    '''

    @staticmethod
    def one():
        path = "C:\\Users\\sujee\\Desktop\\spark_input\\bands\*.json"
        spark = get_spark_session()
        df = spark.read.format('json').option('mode', 'permissive').option('header', 'true').option('inferSchema',
                                                                                                    'true') \
            .option('path', path).load()
        df.show()


def working_with_data_sources():
    obj = WorkingWithDataSources()
    obj.one()


"""

mode==> permisssive
+--------------------+-----------+----+------------+----+
|     _corrupt_record|   hometown|  id|        name|year|
+--------------------+-----------+----+------------+----+
|                null|     Sydney|   1|       AC/DC|1973|
|                null|     London|   0|Led Zeppelin|1968|
|                null|Los Angeles|   3|   Metallica|1981|
|{"id":4,"name":"T...|       null|null|        null|null|
+--------------------+-----------+----+------------+----+

"""
