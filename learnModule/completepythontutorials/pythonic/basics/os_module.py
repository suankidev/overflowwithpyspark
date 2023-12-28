import os

# print(dir(os)) print all available method

print(os.getcwd())

os.chdir('C:\\Users\\sujee\\Desktop\\spark_input')

print(os.getcwd())

print(os.listdir())


'''create dir'''


try:
    os.mkdir('os-demo')
    os.makedirs('demo1\\demo2\\demo3')

except FileExistsError as e:
    print(os.listdir())
    #os.remove('os-demo') #works on files
    os.rmdir('os-demo')
    #os.removedirs('demo1/demo2/demo3')



print(os.listdir())


