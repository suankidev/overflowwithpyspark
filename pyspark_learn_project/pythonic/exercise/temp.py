class X:
    a = 10

    def __init__(self):
        print('inside X', X.a)
        print('inside x constructor')

    @classmethod
    def m1(cls):
        print('X static method')

    @staticmethod
    def m2():
        print('X static method')


class Y(X):
    b = 20

    def __init__(self) -> None:
        print(Y.b)
        print(Y.a)
        Y.m1()
        Y.m2()
        #super().__init__()


Y()
