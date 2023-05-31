print("=" * 50)

dir_path = r"/logs"
play_dir= r"/playground"




file_path= dir_path+r'\test6.txt'


with open(file_path,'r+') as writer:
    lines = writer.readlines()
    print(lines[1])

print()
print("=" * 50)
