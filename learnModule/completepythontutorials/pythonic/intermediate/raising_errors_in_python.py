class Car:
    def __init__(self, make, model):
        self.make = make
        self.model = model

    def __repr__(self):
        return f"<Car {self.make} {self.model}>"


class Garage:
    def __init__(self):
        self.cars = []

    def __len__(self):
        return len(self.cars)

    def add_cars(self, car):
        """runs only when car is object of Car"""
        if not isinstance(car, Car):
            raise NotImplementedError(
                f"Tried to add {car.__class__.__name__} to the garage , but Only `Car` objects can be added ")

        self.cars.append(car)

    def __repr__(self):
        return f"<Garage {self.cars}>"


fiesta = Car("fiesta", "Z6plus")
jackal = Car("jackal", "z8")
ford = Garage()

try:
    ford.add_cars(fiesta)
    ford.add_cars(jackal)
except TypeError:
    print("Your car is not a Car")


"""Custom error """



class MyCustomeErro(TypeError):
    """
    My customer error is raised when value is not matched
    """
    def __init__(self,msg,code):
        super().__init__(f"{msg} and {code}")





def test_error(id):
    """
    :param
    :return: print value info
    """
    if id == 980:
        print("ok")
    else:
        raise MyCustomeErro("value is not correct", 501)


#test_error(278)

err = MyCustomeErro("valus is not matched",501)
print(err.__doc__)






def powr_of_two():
    user_input = input("Enter the number: ")

    try:
        n = float(user_input)
    except ValueError:
        print("You input was invalid")
        n = 0
    else:
        n_sequare = n ** 2
    finally:
        return n_sequare





