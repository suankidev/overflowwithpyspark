#%%

"""
*IndexError
*KeyError
*NameError
*AttributeError
*NotImplementedError
*RuntimeError
*SyntaxError
*IndentationError
*TabError
*TypeError
*ValueError
*DepricationWarning


python exception hierarchy:

                        BaseException
                            ||
----------------------------------------------------------
||                ||              ||                     ||
Exception     SystemExit   GeneratorExit             KeyBoradInterrupt
||
AttributeError
ArithmaticError---->>ZeroDivisionError,FloatingPointError,OverflowError
EOFError
NameError
LookupError---->>IndexError,KeyError
OSError-->>FileNotFoundError,InterruptedError, PermissionError,TimeOutError
TypeError
ValueError
"""
#%%

#IndexError

friends = ['Sujeet','Kamesh']

friends[2]


#%%

#KeyError


friends = {'name':'Sujeet','age':23}

friends['test']

#%%


#NameError

print(hello)


#%%
#AttributeError

friends_list = ['Sujeet','Kamesh']


print(friends_list.intersection(['a','b']))


#%%
#NotImplementdError


class User:
    
	def __init__(self,username) -> None:
		self.username = username
    
	def login(self):
		raise NotImplementedError('This feature has not implented yet! ')




#%%

#RuntimeError

"""baseclass of error"""





#%%

#syntaxt error

class Test
	def __init__(self) -> None:
		pass

#%%

class Car:
    def __init__(self, make, modle):
        self.make = make
        self.model = modle

    def __repr__(self):
        return f"<Car {self.make} {self.model}>"


class Garage(Car):
    def __init__(self):
        self.cars = []

    def __len__(self):
        return len(self.cars)

    def add_car(self, car):
        # raise NotImplementedError("We can't add cars to garage yet.")
        if not isinstance(car, Car):
            raise TypeError(
                f"Tried to add a '{car.__class__.__name__}' to the garage, but you can only add 'Car' Object.")
        self.cars.append(car)


ford = Garage()
car = Car('ford','fiesta')
car1 = Car('ford','air')
ford.add_car(car)
ford.add_car(car1)

print(ford.cars)

#%%







