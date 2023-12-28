# Object oriented programming

class Employee:
    pass


emp_1 = Employee()
emp_2 = Employee()

print(emp_1)
print(emp_2)

emp_1.first = 'Sujeet'
emp_1.last = "sing"
emp_1.salary = 23000

emp_2.first = 'Test'
emp_2.last = "test"
emp_2.salary = 4343

print(emp_1.salary)
print(emp_2.salary)


# doing same thing with init method

class Emp:
    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay

    def getfullname(self):
        """self is required to take emp_1 as argument
        automatically """
        return f"{self.first}  {self.last}"


emp_1 = Emp('Sujeet', 'Kumar', 34000)
emp_2 = Emp('Demo', 'last', 70000)

print(emp_1.first)
print(emp_1.getfullname()) #is basically Employee.getfullname(emp_1)