

print("Hello from module -->",__name__)

def save_to_file(content, filename):
    with open(file=filename, mode='w') as file:
        file.write(content)


def read_file(filename):
    with open(file=filename) as file:
        print(file.readlines())


