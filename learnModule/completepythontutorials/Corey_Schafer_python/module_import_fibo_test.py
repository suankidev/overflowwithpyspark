
import fibo


fibo.fib(10)



print("="*30)

print(fibo.__name__)
print(__name__)

print("="*30)
# to relad module again
import importlib;
importlib.reload(fibo)


# The Module Search Path

import sys
print(f"Sys.path=>{sys.path}")

print("="*30)

print(f"""When module name fib is imported it first search in 
{sys.builtin_module_names}""")


print("="*30)


print(f"What if we import moduletests and calculator.py from another package")

from moduletests.calculator import sum


print(f"Sys.path=>{sys.path}")

print(f"""When module name fib is imported it first search in 
{sys.builtin_module_names}""")


print("sum of {} and {} is {}".format(10,20,sum(10,20)))

print("="*30)


import os

# print(f"python path is {os.environ['PYTHONPATH']}")



print("="*30)


print(type(sys.path))

sys.path.append("/home/suanki") #without this import test_Sys_path would fail

import test_sys_path

print(f"Sys.path=>{sys.path}")

print("="*30)


__doc__ = """The variable sys.path is a list of strings that determines the
 interpreter’s search path for modules. It is initialized to a default
  path taken from the environment variable PYTHONPATH,
   or from a built-in default if PYTHONPATH is not set. 
   You can modify it using standard list operations:"""



print(__doc__)

print("="*30)


dir_name = """The built-in function dir() is used to
 find out which names a module defines.
 It returns a sorted list of strings:"""


print(dir_name)


print(dir(fibo))

print("="*30       )



print("""Without arguments, dir() lists the names you have defined currently:""")

print(dir())

print("="*30)


import builtins

print(dir(builtins))

print("="*30)




print("Module and submoduel test")

print(f"Sys.path=>{sys.path}")
print(f"dir:=> {dir()}")

from moduletests.package2.subtract_package2 import show_info, add_20

print(show_info())





print(f"add_20-> {add_20(30)}")
