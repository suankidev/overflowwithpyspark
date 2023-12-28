import pandas as pd
import numpy as np


def read_file():
    path = r"C:\Users\sujee\Desktop\spark_input\stack-overflow-developer-survey-2019"
    df = pd.read_csv(path + r"\survey_results_public.csv", index_col='Respondent')
    df_schema = pd.read_csv(path + r"\survey_results_schema.csv", index_col='Column')
    pd.set_option('display.max_columns', 85)
    pd.set_option('display.max_rows', 85)

    return df, df_schema


from common_utils.sparkUtils import get_spark_session

def basic():
    #d = {'col1': [1, 2], 'col2': [3, 4]}
    d = [(1,2),(3,4)]
    ps = get_spark_session()
    df = ps.createDataFrame(d,['col1','col2'])
    df.show()




if __name__ == '__main__':
    print("started panada..")
    basic()
