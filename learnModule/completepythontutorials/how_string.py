name = "Sujeet"
final_greet = "How are you ,{}"

sujeet_greet = final_greet.format(name)

print(sujeet_greet)

name = "ramesh"
ramesh_greet = final_greet.format(name)

print(ramesh_greet)


#%%
name = "Siya"
another_greeting = f"How are you {name}"

print(another_greeting)

name = "bipolar"

print(another_greeting)  #won't affect here  o/p Siya

#%%


description = "{} is {age} years old."

print(description.format("Bob",age=20))
"""
make sure..with name goes after once without name is give in format
"""


#%%

my_name = "Sujeet"

#alwys o/p string hence need to convert to int
age = int(input("Enter you age:"))

print(f"you have live for {age * 12} months")



#%%






#%%









