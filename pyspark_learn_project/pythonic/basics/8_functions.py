def hello_func():
    print("Hello function!")

hello_func()

#if we don't put () paranthesis whil calling the it represent
#function not

print(hello_func)

#keep you code dry: don't repeat yourself so use function


def hello_func1(greeting,name='you'):
    return '{}, {} '.format(greeting,name)

rslt = hello_func1('hello')

print(rslt)

def student_info(*args,**kwargs):
    print(args)
    print(kwargs)

courses = ('Math', 'Arts')
info = {'name': 'sujeet', 'age': 22}

student_info('Math','Arts',name='sujeet',age=22)

student_info(courses,info)
student_info(*courses,**info) #is equailant to first call of student_info




