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
    def create_emp(cls, emp_str):
        # return Emp(first,last,pay)
        first, last, pay = emp_str.split('-')
        return cls(first, last, int(pay))

    @staticmethod
    def is_workday(day):
        raise_amount == 1.6
        if day.weekday() == 5 or day.weekday() == 6:
            return False
        return True


# inhrited emp

class Developer(Emp):
    raise_amount = 1.20

    def __init__(self, first, last, pay, programming_lan):
        super().__init__(first, last, pay)
        self.programming = programming_lan


class Manager(Emp):
    def __init__(self, first, last, pay, employee=None):
        super().__init__(first, last, pay)
        if employee is None:
            self.employee = []
        else:
            self.employee = employee

    def add_emp(self, emp):
        if emp not in self.employee:
            self.employee.append(emp)

    def remove_emp(self, emp):
        if emp in self.employee:
            self.employee.remove(emp)

    def print_emp(self):
        for i in self.employee:
            print('-->', i.getfullname())


em1_str = 'sujeet-singh-8000'
dev_1 = Emp.create_emp(em1_str)

dev_one = Developer('sujeet', 'Singh', '7000', 'Python')
print(dev_1.pay)
dev_1.apply_raise()
print(dev_1.raise_amount)
print(dev_1.pay)

print(dev_1.getfullname(), dev_1.pay)

# method resolution order
# print(help(Developer))

print(dev_one.getfullname(), dev_one.programming)

# manager

maneger1 = Manager('vaibha', 'Halbe', 4000, [dev_1, dev_one])

# maneger1.add_emp(dev_1)

print(maneger1.print_emp())

#check first is related to second

print(isinstance(maneger1,Emp)) #check if manager is instance of Emp
print(isinstance(maneger1,Developer)) #check if manager1 is instance of Developer


print(issubclass(maneger1,Developer))