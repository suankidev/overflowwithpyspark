import json

"""
json allow to use dict, list but not tuple
"""

with open("friends.json","r") as reader:
    file_content = json.load(reader)



print(file_content)

print(file_content['friends'])

#writting json

cars = [
    {'make' : 'Ford','model': "Fiesta"},
    {"make" : "Mahindra","model":"Beloro"}
]


with open("cars.json","w") as writter:
    json.dump(cars,writter)




