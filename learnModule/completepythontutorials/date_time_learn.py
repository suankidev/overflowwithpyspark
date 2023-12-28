import time
from datetime import datetime,timezone, timedelta
from dateutil.tz import gettz


def date_time():
    #current time
    #print(datetime.now())  #naiv object no info on timezone
    naive_time = datetime.now()
    aware_time = datetime.now(tz=timezone.utc)  #+5 offset from UTC to IST

    print(naive_time)
    print(aware_time)
    #printing local time from aware_time
    ist=gettz("Asia/Kolkata")
    print(aware_time.now(tz=ist))

    #wotking with delta
    print()
    tomorrow = aware_time + timedelta(days=1)
    print(tomorrow)

    #formatting string
    print(tomorrow.strftime('%d-%m-%Y %H:%M')) #datetime to string

    my_date="20221230"
    user_date=datetime.strptime(my_date,'%Y%m%d') #string parse time
    print(user_date)

    print(user_date.astimezone(timezone.utc))



def power(limit):
    return [x**2 for x in range(limit)]


def measure_runtime(func):
    start = time.time()
    func()
    end = time.time()
    print(end - start)




measure_runtime(lambda :power(50000))

import timeit

#print(timeit.timeit("[x**2 for x in range(1000)]"))



