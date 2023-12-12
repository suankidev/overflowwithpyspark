from datetime import datetime
import os

"""


"""

#print current working dir
print(os.getcwd())

os.chdir("C:/Users/sujee/OneDrive/Documents/surendra/")

print(os.getcwd())

print(os.listdir())

# os.mkdir('test_dir')
# os.makedirs('test/test_dir')
# print(os.listdir())
# os.rmdir('test_dir')
# os.removedirs('test/test_dir')
# print(os.listdir())

mod_time=os.stat('icerties').st_mtime

print(datetime.fromtimestamp(mod_time))
#print(dir(os))


  #generator path dirinpath fileinpath 
for dirpath, dirnames, filename in os.walk(os.getcwd()):
	print('Current path:', dirpath)
	print('Directories:', dirnames)
	print('Files:', filename)


print("=="*20)	        


print(os.environ.get('MYLIB'))

#os path module is good to work with path
file_path=os.path.join(os.environ.get('MYLIB') , 'test.txt')

print(file_path)


print(os.path.basename(file_path))
print(os.path.dirname(file_path))
print(os.path.split(file_path))
print(os.path.exists(file_path))
print(os.path.isfile(file_path))
print(os.path.splitext(file_path))










