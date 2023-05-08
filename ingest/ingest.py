import sys,os
pwd=os.getcwd()
sys.path.extend([pwd])

from flowutils.sparkUtils import get_spark_session




if __name__ == '__main__':
    
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    print('HI')

    print(sys.executable)
    print(get_spark_session())
