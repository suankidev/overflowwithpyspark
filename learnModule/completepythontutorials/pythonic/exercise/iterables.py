#%%
"""
for name in iterable:
    print name
"""
import enum
import re
import os
import itertools
from typing import Iterator

name=['pratik','rohit','suraj','varda','rohan','nilesh','subham','vishal']
grade=[100,200,300,400,500,600,700,800]
height={0:175,1:145,2:134,3:138,4:156,5:190,6:167,7:140,8:133}
education=('BE','MBA','BCA','BTECH','MCA','PGDM','MASS COMM','MSC')

print(dir(name))
 
class myrange(object):
    def __init__(self,start,end) -> None:
        self.value=start
        self.end=end
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self.value >= self.end:
            raise StopIteration
        current=self.value
        self.value += 1
        return current 

nums=myrange(1,10)

for i in nums:
    print(i)

print('='*80)
#%%
print('='*50)
################################################

print(dir(name))
#myitr=iter(name)
myitr=name.__iter__()
while True:
    try:
       print(next(myitr))
    except StopIteration:
        print()
        break



################################################
print('='*50)
#%%

################################################
#__iter__ generators

class toDoList(object):
    def __init__(self):
        self.tasks=[]

    def __iter__(self):
        for task in self.task:
            if not task.done:
                yield task
    def all(self):
        return iter(self.tasks)

    def done(self):
        return (t for t in self.tasks if t.done)

print('='*50)
#%%
print('='*50)
################################################

#make your own object iterable

class ToDoList(object):
    def __init__(self):
        self.tasks=[]
    
    def __iter__(self):
        return iter(self.tasks)

todo=ToDoList()

for task in todo:
    print(task)
################################################
"""low level iterator
iterable : produces an iterator
iterator: produces a stream of values

iterator==iter(iterable)  #iterable.__iter__()
value=next(iterator)   #iterator.next() or .__next__()
value=next(iterator)
...
..
..
only operation on iterator is next
"""

with open('../durga_classes/emp.csv','r') as f:
    header_line=next(f)
    print(header_line)
    #pics from where you left
    for i in f:
        print(i) 
#############################################
#make the double loop single
#iterate throug row and column
#x is row and y will be the column
def foo(x,y):
    for i in range(x):
        for j in range(y):
            yield i, j

for i, j in foo(10,20):
    if i==5:
        break
    #no way to break in double lopp but this is way by ssing generator
    print(i,j)

################################################
#abstacting your iteration
f=open('../durga_classes/aadi.py','r')


def filegen(f):
    for line in f:
        line=line.strip()
        if line.startswith('#'):
           continue
        if not line:
           continue
        yield line

print()
for i in filegen(f):
    print(">",i)
for line in f:
    #print(line)
    line=line.strip()
    if line.startswith('#'):
        continue
    if not line:
        continue
    print(line)
################################################
####################################33

#generators
#function return one value = Generators produce a stream

def myfunc():
    yield 'hi'
    yield 'hello'

print(type(myfunc()))

for x in myfunc():
    print(x)


#how to make our even function generator
num=[x for x in range(1,100)]

def evengen(num):
    for n in num:
        if n % 2==0:
            yield n 
            
for i in evengen(num):
    print(i)
    #do_something(i)

#######################################

##########################
#custom looping
def do_something(n):
    pass

for g in grade:
    if n%2==0:
        do_something(n)


#or
even=[]
def evens(stream):
    for i in stream:
        if i % 2==0:
            even.append(i)
    
    return even


for i in even:
    do_something(i)
#########################
#dict() accept a stream of pairs
print(dict(enumerate(name)))
print(dict(zip(name,education)))

print(max(height.values()))

print(max(height.items(),key=lambda x:x[1]))
print(max(height,key=height.get))

def myfun(*arg):
    #print(list(enumerate(arg)))
    pass

myfun(name,education)

#zip() make pair wise loop or geneated stream of pairs 
for i,j in zip(name,education):
    print(i,j)

f=open('./iterables.py','r')
#will return key and value
for it in enumerate(f,start=1):
    print(it)

for ind,value in enumerate(f,start=1):
    print(it,value)


print(list(enumerate(name)))

#enumerate through keys only for dict
print(list(enumerate(height)))
print(list(enumerate(education)))
print('-'.join(name))

#key will be new list
new_list=list(height)
print(new_list)

def f(x):
    print(x)

new_list1=[f(x) for x in new_list]
rslt=sum(new_list)
print(rslt)
#fun(iterables)
print(min(new_list))
print(max(new_list))


from itertools import chain, repeat,cycle, zip_longest
seq=chain(repeat(17,3),cycle(range(45)))
for num in seq:
    print(num)
    


for i in itertools.count():
    #once sarted count upto infinite
    print(i)




for root , dirs, files in os.walk('../'):
    print(root)
    print(dirs)
    print(files)
for match in re.finditer('[0-9]*','hi897'):
    print(match)
for k in height.keys():
    print(k)
for v in height.values():
    print(v)
for k,v in height.items():
    print(k,v)

print('='*50)

#%%



print('='*50)
for i in name:
    print(i)

print('='*10)

#for index
for i,j in enumerate(name):
    print(i, j)












#%%

with open('C:/Users/sujee/Desktop/python_playground/durga_classes/menu.csv','r') as f:
    for i in f:
        print(repr(i))

