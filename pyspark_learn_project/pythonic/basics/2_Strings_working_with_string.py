message = 'Hello World'

print(message)

message1 = "Bobby's World"
#or
message1 = 'Bobby\'s World'

#multiline string
message1 = """this is first line
this is second line
    this is third line"""
print(message1)

print(len(message))

print(message[0],message[0:5])
#range  inclusive and exclusive
print(message[0:5],message[:5],message[6:])


#method --methos is a function which belongs to an object
print(message.upper())
print(message.count('l'))  #count the no of times l came in message
print(message.find('l'))  #find the l otherwise it return -1
temp = message.replace('hello','hello sujeet')
print(temp)


greeting = 'Hello'
name = 'Michael'

message = greeting + ', ' + name + '. Welcome!'
#easy to write longer string with this
message = '{}, {}. Welcome!'.format(greeting,name)

print(message)


#using f string  (you can also write code)
message = f'{greeting},{name.upper()}. Welcome!'

print(message)

#all the attribute available to the variable
print(dir(message))

#help on str
print(help(str))

#help with the method on str
print((help(str.lower)))