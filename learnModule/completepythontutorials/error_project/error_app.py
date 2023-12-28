class CarError(Exception):
    """
    Error raised when value is not a Car
    """

    def __init__(self, msg, code):
        super().__init__(msg + str(code))
        self.code = code


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

    def print_car(self, car):
        if not isinstance(car, Car):
            err = CarError(f"{car.__class__.__name__} is not a 'Car' Object. ", 500)
            # print(err.__doc__)
            raise err
        return f"CAR DETAILS : {car.make} {car.model}"


ford = Garage()
fiesta = Car('ford', 'fiesta')
storm = Car('ford', 'storm')

try:
    ford.add_car(fiesta)
except TypeError:
    print("Your car was no a Car!")


