# %%


# %%

class FirstHundredGenerator:
    def __init__(self):
        self.number = 0

    def __next__(self):  # allow to call next
        if self.number < 100:
            current = self.number
            self.number += 1
            return current
        else:
            raise StopIteration()  # reach end of generator


class FirstHundradeIterable:
    def __iter__(self):
        return FirstHundredGenerator()

print(sum(FirstHundradeIterable()))
class AnotherIterable:
    def __init__(self):
        self.cars = ['Fiesta', 'focus']

    def __len__(self):
        return len(self.cars)

    def __getitem__(self, item):
        return self.cars[item]


"""iterator is not iterable
iterator --> we can call next
"""


class PrimeGenerator:
    def __init__(self, stop):
        self.stop = stop
        self.start = 2

    def __next__(self):
        for n in range(self.start, self.stop):  # always search from current start (inclusive) to stop (exclusive)
            for x in range(2, n):
                if n % x == 0:  # not prime
                    break
            else:  # n is prime, because we've gone through the entire loop without having a non-prime situation
                self.start = n + 1  # next time we need to start from n + 1, otherwise we will be trapped on n
                return n  # return n for this round
        raise StopIteration()  # this is what tells Python we've reached the end of the generator

# %%
#
# def prime_generator(bound):
#     flag = False
#     for user_number in range(2, 100):
#         for number_before in range(2, user_number):
#             if user_number % number_before == 0:
#                 flag = False
#                 break
#             else:
#                 flag = True
#         if flag:
#             yield user_number
#
#
# def prime_generator_one(bound):
#     for user_number in range(2, 100):
#         for number_before in range(2, user_number):
#             if user_number % number_before == 0:
#                 break
#         else:
#             yield user_number
#
#
# g = prime_generator_one(100)
#
# for i in g:
#     print(i)


# %%

# def hundred_number():
#     i = 0
#     while i < 50:
#         yield i
#         i += 1
#
#
# g = hundred_number()
# print(next(g))
#
# print(next(g))
#
# print(list(g))
