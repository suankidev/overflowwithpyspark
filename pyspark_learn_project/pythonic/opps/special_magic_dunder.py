class Employee:
    raise_amount = 1.04

    def __init__(self, first_name, last_name, salary):
        self.first_name = first_name
        self.last_name = last_name
        self.pay = salary
        self.email = f"{self.first_name + self.last_name}@gmail.com"

    def getfullname(self):
        return f"{self.first_name + ' ' + self.last_name}"

    @classmethod
    def create_emp_from_str(cls, emp_sting):
        first_name, last_name, salary = emp_sting.split('-')
        return cls(first_name, last_name, salary)

    def __repr__(self):
        return "Employee('{}', '{}', '{}'".format(self.first_name, self.last_name, self.pay)

    def __str__(self):
        return "{} - {}".format(self.getfullname(),self.email)

    def __add__(self,other):
        return self.pay + other.pay

employee1 = Employee('sujeet', 'singh', 34793)
employee2 = Employee('ramesh', 'kumar', 4478)


print(employee2, employee1)  #it;s calling __repr__ method , that how it represet Class name
print(repr(employee2), repr(employee1))
# print(employee2.__repr__())
# print(employee2.__str__())
print(str(employee2))


#print(employee2 + employee1)

#behaviour based on object

# print(1+2)
# print('1'+'2')
