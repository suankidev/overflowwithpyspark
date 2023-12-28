
friends = ["Sujeet","Siva","Rama","Tathcharya","Gundappa","govind","Krishndevroy","Raka"]


start_with_r = filter(lambda x:x.startswith('R'), friends)
#it return generator

another_start_wit = (f for f in friends if f.startswith('R'))  #doing same in here genrator


def my_custom_filter(func, iterable):
    for i in iterable:
        if func(i):
            yield i

print(next(start_with_r))
print(next(start_with_r))