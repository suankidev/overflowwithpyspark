
#%%


age = 20

is_over_age = age >= 20
is_under_age = age <= 20



print(is_over_age)


#%%


age = 20

result =  age < 20 and age > 10

print(result)



bool(0)  #False
bool(13) #True

bool("") #False , empty string

bool("Hello") #True

bool([]) #emptry list, false


#%%

"""
or-> check value before or if it evluates to flase it return after or value

"""

default_greeting = "there"
name = input("Enter you name: (optional) ")

user_name = name or default_greeting


print(f"Hello , {user_name}")

#%%
x = True

cmp = x and 18

print(cmp)  #18  we can any value in and ,or


#%%


age = 16

side_job = True


print(age > 18 and age < 65 or side_job)


""" the condinals evaluate first, and then 'and' and 'or' 
evaluates
left to right

1. False and True or True
2. False and True
3. True

"""

#%%









