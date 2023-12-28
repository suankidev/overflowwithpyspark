from .common.file_operations import read_file

"""
when you run find.py then
dot(.) becom __main__ , which mean __main__.common.file_operations import read_file
which is absolutely not going to work
"""
print("from find.py",__name__)
def find_in(iterable, finder, expected):
    for i in iterable:
        if finder(i) == expected:
            return i
    raise NotFoundError(f"{expected} not found in provided iterable")


class NotFoundError(Exception):
    pass


#read_file('data_1.txt')