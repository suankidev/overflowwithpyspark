import pyspark.sql.session as s

file_path=r"C:\Users\sujee\Desktop\spark_input"
def get_spark_session():
    master="spark://192.168.56.1:7077"
    spark = (s.SparkSession.builder.master(master)
             .appName('spark-optimization')
             .getOrCreate()
             )
    return spark





def flight_df():
    spark = get_spark_session()
    flight_df = spark.read.csv(header=True,path=f"{file_path}\\stockoverflow")
    print("count of stock overflow data ---->",flight_df.count())