import json

file = open("fruites.json", 'r')
fruits = json.load(file)  # read file and turns it to dictionary
file.close()

print(fruits['Fruits'][0])  # {'fruit': 'Apple', 'size': 'Large', 'color': 'Red'}
print(fruits[
          'Fruits'])  # [{'fruit': 'Apple', 'size': 'Large', 'color': 'Red'}, {'fruit': 'Apple', 'size': 'Large', 'color': 'Red'}, {'fruit': 'Apple', 'size': 'Large', 'color': 'Red'}]

print(
    fruits)  # {'Fruits': [{'fruit': 'Apple', 'size': 'Large', 'color': 'Red'}, {'fruit': 'Apple', 'size': 'Large', 'color': 'Red'}, {'fruit': 'Apple', 'size': 'Large', 'color': 'Red'}]}

complex_file = open('question.json', 'r')
question = json.load(complex_file)

complex_file.close()

print(question["quiz"])

cars = [
    {"make": "ford", "model": "fiesta"},
    {"make": "ford", "model": "storm"}
]

f = open('cars.json', 'w')
json.dump(cars, f)
f.close()

# load json string directly
json_string = '[{"make": "ford", "model": "fiesta"}, {"make": "ford", "model": "storm"}]'

incorrect_car = json.loads(json_string)

print(incorrect_car[0])  # {'make': 'ford', 'model': 'fiesta'


