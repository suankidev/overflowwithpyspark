"""
class variable: shared by all instance
instance variable: diffrent for diff insance

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

emp_1 = Emp('Sujeet', 'Kumar', 34000)
emp_2 = Emp('Demo', 'last', 70000)

print(emp_1.first)
print(emp_1.pay)

emp_1.apply_raise()

print(emp_1.pay)


print(emp_1.raise_amount) #firs raise_amount will be searched in instance variable then class
print(Emp.raise_amount)

#printing the namespace variable availbe on instance

print(emp_1.__dict__)
#{'first': 'Sujeet', 'last': 'Kumar', 'pay': 35360}
print(Emp.__dict__)
#{'__module__': '__main__', 'raise_amount': 1.04, '__init__': <function Emp.__init__ at 0x000002B338BD3550>, 'getfullname': <function Emp.getfullname at 0x000002B338BD35E0>, 'apply_raise': <function Emp.apply_raise at 0x000002B338BD3670>, '__dict__': <attribute '__dict__' of 'Emp' objects>, '__weakref__': <attribute '__weakref__' of 'Emp' objects>, '__doc__': None}


#will change for all
Emp.raise_amount = 2.6

print(emp_1.pay)
#will change only for emp_1
emp_1.raise_amount = 7.9

print(emp_1.__dict__)
print(emp_2.__dict__)
""""{'first': 'Sujeet', 'last': 'Kumar', 'pay': 35360, 'raise_amount': 1.06}
{'first': 'Demo', 'last': 'last', 'pay': 70000}"""

emp_1.apply_raise()

print(Emp.raise_amount)
print(emp_1.pay)

print(Emp.no_of_emps)