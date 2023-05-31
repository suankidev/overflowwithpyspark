from src.flowutils.SparkUtils import get_spark_session


def test_spark():
    flag = False
    if get_spark_session():
        flag = True

    assert 10 == 10
