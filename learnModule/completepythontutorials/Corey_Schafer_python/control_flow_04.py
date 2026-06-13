#%%

x = input("Enter a number: ")

print(x)
print(type(x))



#%%


x = 10

if x < 0:
    print("Negative number")
elif x > 0:
    print("Positive number")
else:
    print("Zero")






#%%

words = ['cat', 'window', 'defenestrate']

for word in words:
    print(word,len(word))



#%%

users = {'Hans': 'active', 'Éléonore': 'inactive', '景太郎': 'active'}

for user,status in users.copy().items():
    if status == 'inactive':
        del users[user]


print(users)

# %%

for i in range(10):
    print(i)

#%%

type(range(10))



#%%

list(range(10))


#%%



a = ['Mary', 'had', 'a', 'little', 'lamb']

for i in range(len(a)):
    print(a[i])
# %%


for i,j in enumerate(a):
    print(i, j)


#%%


for i in range(10):
    for j in range(5):
        if j == 3:
            break
        print(i,j)
# %%

for i in range(2,10):
    for j in range(2,i):
        if i % j == 0:
            break
    else:
        print(i)
# %%

i = 1
while i < 10:
    if i % 2 == 0:
        break
    print(i)
    i += 1
else:
    print(i)

# %%


def http_error(status):
    match status:
        case 200 | 207 | 208:
            return "Success"
        case 400:
            return "Bad request"
        case 404:
            return "Not found"
        case 418:
            return "I'm a teapot"
        case _:
            return "Something's wrong with the internet"


http_error(400)

#%
# %%


# point is an (x, y) tuple
def get_position(point):
    match point:
        case (0, 0):
            print("Origin")
        case (0, y):
            print(f"Y={y}")
        case (x, 0):
            print(f"X={x}")
        case (x, y):
            print(f"X={x}, Y={y}")
        case _:
            raise ValueError("Not a point")



get_position((0,0))
get_position((10,20))



# %%
class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

def where_is(point):
    match point:
        case Point(x=0, y=0):
            print("Origin")
        case Point(x=0, y=y):
            print(f"Y={y}")
        case Point(x=x, y=0):
            print(f"X={x}")
        case Point():
            print("Somewhere else")
        case _:
            print("Not a point")

p1 = Point(10,20)

type(p1)

where_is(p1)

where_is(Point(10,0))

# %%

from enum import Enum

class Color(Enum):
    RED = 'red'
    GREEN = 'green'
    BLUE = 'blue'


    
Color.RED.name
Color.RED.value


# %%


def get_color_value(color):
    match color:
        case Color.RED:
            print("I see red!")
        case Color.GREEN:
            print("Grass is green")
        case Color.BLUE:
            print("I'm feeling the blues :(")


get_color_value(Color('red'))

Color('green').value


# %%


i = 5

def f(arg=i):
    print(arg)

i = 6
f()


# %%


l = [1,23,4]

def append(a, mylist=l):
    mylist.append(a)
    print(mylist)


l = [6,7,9]

append(10)


# %%


def f(a, myList= None):
    if myList is None:
        myList = []
    myList.append(a)
    return myList



print(f(10))
print(f(22))
# %%


# keyword argument

def f(**keyArgs):
    print(type(keyArgs))
    print(keyArgs)



f(**{"name":"Sujeet","age":"30"})
f(name="Sujeet",age="22",country="India")


# %%



def check(*t):
    print(type(t))
    print(t)


check(10,20,30)


#%%


"/".join(("earth","mars"))




args = [3, 6]


check(*args)
check(10,20,40)
check(args)

#%%


pairs = [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')]


pairs.sort(key=lambda x: x[1])

print(pairs)
# %%

mytuple = ("apple", "banana", "cherry","apple")

type(mytuple)
mytuple[0]
# %%




def my_function():
    """Do nothing , but document it"""
    pass


my_function.__doc__



#%%


def cook(veg, dal="Arhar", rice="White"):
    print(cook.__annotations__)



cook("Couliflower")




# %%


def f(veg: str, dal: str="Arhar", rice: str="White") -> str:
    print(f.__annotations__)



f("Couliflower")


# %%
