"""Built in Errors in python

IndexError
KeyError
NameError
AttributeError
NotImplementedError
RuntimeError
SyntaxError
IndentationError
TabError
TypeError ==. 5 + 'five'
ValueError  ==> int(20.5)   will raise valueError
ImportError ===> circuiler import   a --> b  and b --> a  it's  pain in ass
DeprecatoinWarning

"""

friends = ['a','b']
try:
    friends[2] #indexError
except IndexError as e:
    print(e)

friends = {'a':1,'b':2}



try:
    friends['c']
except KeyError as e:
    print(e)



try:
    hello
except NameError as e:
    print(e)


#not implemented error

class User:
    def __init(self,user,password):
        self.user = user
        self.password = password

    def login(self):
        raise NotImplementedError("this feature is not yer implementd!")

    def register(self):
        raise DeprecationWarning("use register_user")

    @classmethod
    def register_user(cls):
        return "better way to register user"

import blog

print(blog)