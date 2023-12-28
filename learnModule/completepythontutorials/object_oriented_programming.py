# %%

"""
students =[
     {"name" : "Rolf","grades":(78,65,27,22)},
     {"name" : "Ramesh","grades":(78,24,27,45)},
     {"name" : "Kamesh","grades":(78,45,27,33)},

]

def student_avg(d):
    grade = d['grades']
    name  = d['name']
    print(f"{name} --> {sum(grade)/len(grade)}")


for i in students:
    student_avg(i) 

"""

# %%

"""
# ------> class concept

class student:
    def __init__(self, name, grade):
        self.name = name    #name is no longer variable it's now called property
        self.grade = grade

    def average(self):   #avg is not longer function, it's now method
        return sum(self.grade) / len(self.grade)

#two different entity, they don't know each other
student_one = student("Kamesh",(78,45,27,33))
student_two = student("Sujeet",(78,76,27,39))

print(student_one.average())
print(student_two.average())

print(student_one.__class__)

# print(student.average(student_one)) # by default pass self when you cal student_one.average()

"""


#%%

#---> magic method in class

"""class Student:
    def __init__(self, name):
        self.name = name



movies  = ["The sunrise","Suryavansham"]

print(movies.__class__)  #<class 'list'>
print(len(movies))

print(Student.__class__)  #<class 'type'>
"""
#%%

"""
class Garagge:
    def __init__(self):
        self.cars =  []

    def __len__(self):
        return len(self.cars)

    def __getitem__(self, item):
        return self.cars[item]


    def __repr__(self):
#        tell user about the
        return f"<Garage {self.cars}"

    def __str__(self):
        #tell user about insight of  object
        return f"<Garage with length of {len(self.cars)}>"

ford = Garagge()

print(ford.cars)
print(ford.__class__)  #<class '__main__.Garagge'>
print(Garagge.__class__) #<class 'type'>

ford.cars.append("fiesta")
ford.cars.append("venture")


print(len(ford))
print(ford[0]) #or print(Garagge.__getitem__(ford,0))

print("\nprinting fords cars")
for car in ford:
    print(car)


print(ford)

"""


#%%


"""Inheritence in python
"""


class Student:
    def __int__(self,name,school):
        self.name= name
        self.school= school
        self.marks=[]

    def average(self):
        return f"average for {self.name} is {sum(self.marks)/len(self.marks)}"

class WorkingStudent(Student):
    def __int__(self,name,school,salary):
        super().__int__(name,school)
        self.salary = salary



sujeet  = WorkingStudent("Sujeet","Self study",1200)
print(sujeet.salary)


#%%
class Student:
    def __init__(self, name, school):
        self.name = name
        self.school = school
        self.marks = []

    def average(self):
        return f"average for {self.name} is {sum(self.marks) / len(self.marks)}"


class WorkingStudent(Student):
    def __init__(self, name, school, salary):
        super().__init__(name, school)
        self.salary = salary

    @property
    def weekly_salary(self):
        return self.salary * 30


print("")
# sujeet = WorkingStudent("Sujeet", "Self study", 1200)
# sujeet.marks.append(85)
# sujeet.marks.append(98)
#
# print(sujeet.average())
# # print(sujeet.weekly_salary())
# print(sujeet.weekly_salary)


class Foo:
    @classmethod
    def hi(cls):
        print(cls.__name__)


my_object = Foo()

my_object.hi()


#%%
from currency_symbols import CurrencySymbols


class FixedFloat:
    def __init__(self, amount):
        self.amount = amount

    def __repr__(self):
        return f"<FixedFloat {self.amount:.2f} >"

    @classmethod
    def from_sum(cls,value1, value2):
        # return FixedFloat(value1 + value2)
        return cls(value1 + value2)


class Euro(FixedFloat):
    def __init__(self, amount):
        super().__init__(amount)
        self.symbol = CurrencySymbols.get_symbol("EUR")

    def __repr__(self):
        return f"<Euro {self.symbol}{self.amount:.2f} >"


class INR(FixedFloat):
    def __init__(self, amount):
        super().__init__(amount)
        self.symbol = CurrencySymbols.get_symbol("INR")

    def __repr__(self):
        return f"<INR {self.symbol}{self.amount:.2f} >"


new_number = FixedFloat(12.456)
print(new_number)

eur_money = Euro(236.2833)
print(eur_money)

inr_money = INR(27923)
print(inr_money)

inr_money = INR.from_sum(676.343 , 674.343)
print(inr_money)


"""
1. use class method in place of static method
or wherever you don't need self

2. use static method only when it's not going to inherited from
b/c when you inherit cls provide more benefits 

"""


#%%




