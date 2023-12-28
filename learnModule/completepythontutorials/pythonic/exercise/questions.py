import sys


def write_to_file():
    import os

    for i in os.listdir("../../resources"):
        if 'txt' in i:
            file_name = i

    file_path = r"../../resources/" + file_name

    print(file_path)

    with open(file_path, 'r') as f:
        print(f.read())

    if os.path.exists(file_path):
        os.remove(file_path)



def string_test():

    d = (1,2,3,4,5,5)

    print(d)

string_test()



print(sys.path)