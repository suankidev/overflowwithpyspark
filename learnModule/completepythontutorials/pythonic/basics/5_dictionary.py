d = {}
d = dict()
print(type(d))
print(dir(d))

student = {'name': 'John','age':25,'courses':['Math','CompSci']}

print(student)

print(student['name'])
#what if key doesn't exist then will get error so use get method

print(student.get('phone','default value'))

#adding a value
student['phone'] = '3333-333' #if it's the then update otherwise add an entry
print(student.get('phone'))

#update mulitiple key or add if not exist
address = {'Home':'leh laddakh','office':'kashmir chowk','phone':'9999-999'}

student.update(address)
print(student)

#remove the value from dict
del student['phone']
student.pop('Home')

print(student)

#loop
print(student.keys())
print(student.items())

for i,k in student.items():
    print(i,k)

for i,k in enumerate(student):
    print(i,k)









