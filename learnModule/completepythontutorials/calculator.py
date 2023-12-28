from addition import Addition


class Calculator:

    @classmethod
    def add(cls, num1, num2):
        return Addition.add(num1, num2)  # make use of add() from addition module

    @classmethod
    def subtract(cls, num1, num2):
        return Addition.add(num1, -num2)

    @classmethod
    def multiply(cls, num1, num2):
        if num1 == 0 or num2 == 0:
            return 0
        else:
            res = 0
            for x in range(0,num2):
                res = Addition.add(res,num1)

            return res


    @classmethod
    def divide(cls,num1, num2):
        res = 0
        while num1 >= num2:
            num1 = cls.subtract(num1, num2)  # subtract num2 from num1 until its remainder is smaller than num2
            res = cls.add(res, 1)  # count the times of subtraction as the result

        return res





print(
    Calculator().divide(11,2)
)