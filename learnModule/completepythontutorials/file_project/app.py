f = open('data.txt','r')
file_content = f.read()

f.close()

print(file_content)


user_input = input("Enter your name: ")
my_file_writing = open('data.txt','w')
my_file_writing.write(user_input)
my_file_writing.close()






