if True:
    print('Conditional was True')

#We can use comparision operator
"""
==
!=
>
<
>=
<=
is  (object identity)

"""

language = 'Python'

if language == 'Python':
    print(f"Language is {language}")
elif language == 'Java':
    print(f"language is {language}")
elif language == 'Java Script':
    print(f"language is {language}")
else:
    print("NO match")


#and
#or
#not

user = 'Admin'
logged_in = False
if user == 'Admin' and logged_in:
    print(f"user {user} is logged in ")
elif user == 'Admin' and not logged_in:
    print(f"user {user} is logged in")


a = [1,2,3]
b = [1,2,3]

print(a == b)
print(a is b)
print(f"id of a is {id(a)} and b is {id(b)}")

if(id(a) == id(b)):
    print("list a and b is same")


#===========evaluate to False
"""
False Value
None
Zero of any nurmeric type
Any emtpy sequence, for example ''. (),{},[],set()
any empty mapping for example, {}
"""

condition = ''

if condition:
    print(f"condition {condition} is evaluated to True")
else:
    print(f"condition {condition} is evaluated to False")

