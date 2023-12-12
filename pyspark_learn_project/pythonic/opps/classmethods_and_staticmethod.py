"""
regular method takes self (instance method)
class method used @classmethod automaically class which cls
static method: they have some logical connection with class
             but not related to instance and class, basically used
             as normal method to perform some operation

"""

class Emp:
    raise_amount = 1.04
    no_of_emps = 0

    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay

        Emp.no_of_emps += 1

    def getfullname(self):
        """self is required to take emp_1 as argument
        automatically """
        return f"{self.first}  {self.last}"

    def apply_raise(self):
        self.pay = int(self.pay * self.raise_amount)

    @classmethod
    def set_raise_amt(cls, amount):
        cls.raise_amount = amount

    @classmethod
    def create_emp(cls,emp_str):
        #return Emp(first,last,pay)
        first, last, pay = emp_str.split('-')
        return cls(first, last, pay)

    @staticmethod
    def is_workday(day):
        if day.weekday() == 5 or day.weekday() == 6:
            return False
        return True

emp_1 = Emp('Sujeet', 'Kumar', 34000)
emp_2 = Emp('Demo', 'last', 70000)

Emp.set_raise_amt(1.05)

print(Emp.raise_amount)
print(emp_1.raise_amount)
print(emp_2.raise_amount)

"""problem statements:
i am getting a set of string want's to create emp out of that
is there a way to do that.
yes: by using class method
"""

emp_str_1 = 'John-Doe-70000'

emp_obj_1 = Emp.create_emp(emp_str_1)

print(emp_obj_1.first)
print(Emp.no_of_emps)

import datetime
mydate = datetime.date(2023, 2, 17)

print(Emp.is_workday(mydate))