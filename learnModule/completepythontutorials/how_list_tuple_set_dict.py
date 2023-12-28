#%%
"""
list--insertion preserverd, duplication allowed, mutable + + +  idm
         [ ]
tuple--insertion preserved, duplication allowed, immutable + + - id-m
          ()

set---insertion not preserved, duplication not allowed, mutable - - +
          indexing and slicing not in set b/c insertion order not preserved
          we can't
          empty set   s=set()
          {}

forzen set--insertion not preserved, duplication not allowed, immutable - - -

dict--oposit to list    duplicate key not allowed

print(type((1, 2, 3,)))

 List
Method	Description
append()	Adds an element at the end of the list
clear()	Removes all the elements from the list
copy()	Returns a copy of the list
count()	Returns the number of elements with the specified value
extend()	Add the elements of a list (or any iterable), to the end of the current list
index()	Returns the index of the first element with the specified value
insert()	Adds an element at the specified position
pop()	Removes the element at the specified position
remove()	Removes the first item with the specified value
reverse()	Reverses the order of the list
sort()	Sorts the list


l = [1,2,3,4]

print(l.sort(), l)
print(sorted(l, reverse=False))
 Dictionary--> A set of key value pair where key should not be duplicate



Method	Description
clear()	Removes all the elements from the dictionary
copy()	Returns a copy of the dictionary
fromkeys()	Returns a dictionary with the specified keys and value
get()	Returns the value of the specified key
items()	Returns a list containing a tuple for each key value pair
keys()	Returns a list containing the dictionary's keys
pop()	Removes the element with the specified key
popitem()	Removes the last inserted key-value pair
setdefault()	Returns the value of the specified key. If the key does not exist: insert the key, with the specified value
update()	Updates the dictionary with the specified key-value pairs
values()	Returns a list of all the values in the dictionary

tuple
Method	Description
count()	Returns the number of times a specified value occurs in a tuple
index()	Searches the tuple for a specified value and returns the position of where it was found

"""

"""set

Method	Description
add()	Adds an element to the set
clear()	Removes all the elements from the set
copy()	Returns a copy of the set
difference()	Returns a set containing the difference between two or more sets
difference_update()	Removes the items in this set that are also included in another, specified set
discard()	Remove the specified item
intersection()	Returns a set, that is the intersection of two or more sets
intersection_update()	Removes the items in this set that are not present in other, specified set(s)
isdisjoint()	Returns whether two sets have a intersection or not
issubset()	Returns whether another set contains this set or not
issuperset()	Returns whether this set contains another set or not
pop()	Removes an element from the set
remove()	Removes the specified element
symmetric_difference()	Returns a set with the symmetric differences of two sets
symmetric_difference_update()	inserts the symmetric differences from this set and another
union()	Return a set containing the union of sets
update()	Update the set with another set, or any other iterable


t =  {1,2,3,4}
t1 = {1,5,6,7}

print(t.difference(t1))  #{2, 3, 4}
print(t.symmetric_difference(t1)) #{2, 3, 4, 5, 6, 7}


len(friends) ==> lenght of list
"""
#%%

friends = ["Rolf","Bob","Anne"]


print(friends[0])
print(len(friends))


friends=[
       ['A','B'],
        ['c','d'],
        ['e','f']
     ]

print(friends)

#%%

friends = ("A","B")

another_friend = friends + ("C",)


print(another_friend)

print(f"{id(friends)} and {id(another_friend)}")




#%%

t =  {1,2,3,4}
t1 = {1,5,6,7}



print("difference", t.difference(t1))
print("diffrence update", t.difference_update(t1), t)

t =  {1,2,3,4}
t1 = {1,5,6,7}

print(t1.discard(1),t1)

t =  {1,2,3,4}
t1 = {1,5,6,7}

print("intersection: " ,t.intersection(t1))
print("intersection update", t.intersection_update(t1),t)


t =  {1,2,3,4}
t1 = {1,5,6,7}

print("symetric diff",t.symmetric_difference(t1))
print("symatric diff update",t.symmetric_difference_update(t1),t)


#%%

friends=(
    {"name":"Rolf Smith","age":24},
    {"name":"Adam Wool","age":30},
    {"name":"Anee Pun","age":27}
)


#converting to a dict

frnd =[('A',20),('B',26)]

print(dict(frnd))




#%%



grades = [80,20,10,40]


sum_of_grades = sum(grades)
size_of = len(grades)

avg = sum_of_grades/size_of

print(avg)







#%%


lottery_numbers = {13, 21, 22, 5, 8}

players = [
    {
        "name":"Sujeet",
        "number":{1,2,4,5}
     },
    {
        "name":"Ramesh",
        "number":{6,7,8,9}
    }
]



name = players[1]["name"]
number = players[1]["number"]

print(f"player {name} got {number.intersection(lottery_numbers)}")

#%%


myval = True and 9 or 6

if True:
    print("Test")
elif False:
    print("False")
else:
    print("NOne")




#%%

 #Ask the user to choose one of two options:
# if they type 'p', our program should print "Hello" and then ask the user again. Keep repeating this until...
# if they type 'q', our program should terminate.
 
# Let's begin by asking the user to type either 'p' or 'q':

user_input = input("Enter P/q : ")


while user_input != "q":
    if user_input == "p":
        print("Hello")
    user_input = input("Enter P/q ")


#%%

currencis = 0.8,9.0

usd,eur=currencis

"""so how it can be used"""

age = [("sujeet",20),("Rajoo",29)]

for name,age in age:
    print(f"{name} is {age} old !")


#%%

age = [("sujeet",20),("Rajoo",29)]
age_dict = {'sujeet': 20, 'Rajoo': 29} #dict(age)


for name,age in age_dict.items():
    print(name,age)


#%%


cars = ["ok","ok","faulty","ok","ok"]

for i in cars:
    if i == "faulty":
        break
    
    print(i)



#%%


# Print out numbers from 1 to 100 (including 100).
# But, instead of printing multiples of 3, print "Fizz"
# Instead of printing multiples of 5, print "Buzz"
# Instead of printing multiples of both 3 and 5, print "FizzBuzz".

for number in range(1,101):    
    if  number % 3 == 0 and number % 5 == 0:
        print("FizzBuzz")
    elif number % 5 == 0:
        print("Buzz")
    elif number % 3 == 0:
        print("Fizz")
    else:
        print(number)


#%%

#find prime number in range of 10


for number in range(2,10):
    for num in range(2,number):
        if number % num == 0:
            break
    else:
        print(number)


#%%

print([_ * 2 for _ in range(5)])
 

for _ in range(10):
    print(_)



#%%
 
#enumerate function

friends = ["Rolf","John","Anna"]

for friend in friends:
    print(friend)


for i,j in enumerate(friends):
    print(i,j)


print(list(enumerate(friends)))


print(list(zip(friends,[0,1,2])))


for i,j in zip(friends,[0,1,2]):
    print(i,j)


#%%


friends = ["Rolf","John","Anna"]

print(dict(enumerate(friends)))
print(list(enumerate(friends)))

print(list(zip([0,1,2,3,4],friends)))





#%%
import random

lottery_numbers = set(random.sample(list(range(22)), 6))

players = [
    {
        "name":"Sujeet",
        "number":{1,2,4,5}
     },
    {
        "name":"Ramesh",
        "number":{6,7,8,9}
    }
]



for i in players:
    name = i['name']
    number = i['number']
    print(f"{name} won {lottery_numbers.intersection(number)}")



#%%



