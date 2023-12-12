'''
LEGB
Local, Enclosing, Global, Built-in

'''
import sys
space = "="*50
print(f"{space}")

#global and local scope

x = 'global x'
p = 'global z'

def test(z):
	global p  #not necessary to define/decalre first line 13
	y = 'local y'
	x = 'local x'
	z = 'local z'
	print(y)
	print(x)
	print(p)
	print(z)


test('local z')
print(x)
print(p)

print(f"{space}  enclosing function outer")

m = min([5,4,2,36,3])

print(m)

import builtins
#print(dir(builtins)) #don't override the existing builtin

def outer():
	x = 'outer x'
	y = 'outer y'

	def inner():
		nonlocal y
		y = 'inner y'
		x = 'inner x'
		print(x)
		print(y)
lemktpr
lemktpr

	inner()
	print(x)
	print(y)


outer()


print("="*50)
