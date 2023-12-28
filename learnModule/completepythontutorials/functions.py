#%%

def greet():
    name = input("Enter you name: ")
    print(f"Hello, {name}")
    


greet()



#%%

cars = [{
    
	"make" : "Ford",
    "model" : "Fiesta",
    "mileage": 23000,
    "fuel_consumed": 460
},
{
    
	"make" : "Ford",
    "model" : "Focus",
    "mileage": 17000,
    "fuel_consumed": 350
},
{
    
	"make" : "Toyata",
    "model" : "Innova",
    "mileage": 1256,
    "fuel_consumed": 546
}
]


def calculate_mpg(car):
    mpg = car["mileage"] / car["fuel_consumed"]
    return mpg


def car_name(car):
	name = f"{car['make']} {car['model']}"
	return name
    

def car_info(car):
     name = car_name(car)
     mpg = calculate_mpg(car)
     print(f"{name} does {mpg} miles per gallon")  

for car in cars:
    car_info(car)






#%%


def devide(x,y):
     if y == 0:
          return "tried to divide by zero"
     else:
          return x / y



print(devide(10,0))
print(devide(0,10))

#%%


#default parameter


def add(x, y=3): #default parameter for y is 3
     total = x + y
     return total
	
print(add(x=3)) #name argument


#%%

divide = lambda x,y: x / y


print(divide(10,5))


(lambda x,y: x / y)(10,5)



#%%


def average(sequnece):
     return sum(sequnece["grades"])/len(sequnece["grades"])


avg = lambda seq: sum(seq) / len(seq)
total = lambda seq: sum(seq)
top = lambda seq: max(seq)

students =[
     {"name" : "Rolf","grades":(78,65,27,22)},
     {"name" : "Ramesh","grades":(78,24,27,45)},
     {"name" : "Kamesh","grades":(78,45,27,33)},
     
]

operation = {
     "avg":avg,
     "total":total,
     "top":top
}


for student in students:
     name = student["name"]
     grades = student["grades"]

     print(f"Student : {name}")

     #operation = "avg"  #or take input from user avg/total/top

     for op in operation:
          opr= operation.get(op)
          print(f"{op} of the {name} is {opr(grades)}")


#%%




def foo(value, fun):
     return fun(value)

rslt=foo(10, lambda x:x*x)


print(rslt)


def over_age(age,getter):
     return getter(age) > 18


print(
     over_age({"Name":"sujeet","age":30}, 
              lambda x:x["age"])
)





















