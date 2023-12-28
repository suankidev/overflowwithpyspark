"""
concern with storing and retriving books from db

Format of the csv file:
name,author,read
"""

# # import json
#
# # books = []
# # books_file = "books.txt"
# books_file = "books.json"

from typing import Union

from completepythontutorials.miltstone_project2.utils.database_connection import DatabaseConnection


def create_book_table() -> None:
    with DatabaseConnection('data.db') as connection:
        cursor = connection.cursor()
        # SUPPORT ONLY FIVE DATA, INTEGER, TEXT, BLOB, NULL,REAL
    cursor.execute('CREATE TABLE  if not exists BOOKS(NAME TEXT,AUTHOR TEXT,READ INTEGER)')

    # with open(books_file, 'w') as file:
    #     json.dump([],file)


def add_book(name: str, author: str) -> None:
    with DatabaseConnection('data.db') as connection:
        cursor = connection.cursor()
        # SUPPORT ONLY FIVE DATA, INTEGER, TEXT, BLOB, NULL,REAL
        cursor.execute("insert into books values(?,?,0)", (name, author))

    # json file--->
    # books = get_all_books()
    # books.append({'Name': name, 'Author': author, 'Read': False})
    # _save_all_books(books)

    # -->csv file
    # with open(books_file, 'a') as file:
    #     file.write(f"{name},{author},0\n")

    # list of books
    # books.append({"Name": name, "Author": author, "Read": False})


Book = dict[str, Union[str, str]]


def get_all_books() -> list[Book]:
    with DatabaseConnection('data.db') as connection:
        cursor = connection.cursor()
        results = cursor.execute("select name,author,read from books")
        books = results.fetchall()  # list of tuples [(name,author,read),(name,author,read)]

    return [{'Name': row[0], 'Author': row[1], 'Read': row[2]} for row in books]
    # json -->
    # with open(books_file, 'r') as file:
    #     return json.load(file)

    # with open(books_file, 'r') as file:
    #     lines = [line.strip().split(",") for line in file.readlines()]  # [[name,author,read],[name,author,read]]
    #
    # return [{"Name": line[0],
    #          "Author": line[1],
    #          "Read": line[2]} for line in lines]


def _save_all_books(books):
    pass

    # with open(books_file, 'w') as file:
    #     json.dump(books, file)

    # with open(books_file, 'w') as file:
    #     for book in books:
    #         file.write(f"{book['Name']},{book['Author']},{book['Read']}\n")


def mark_book_as_read(name: str) -> None:
    with DatabaseConnection('data.db') as connection:
        cursor = connection.cursor()
        cursor.execute(f'update books set read=1 where name="{name}"')

    # books = get_all_books()
    # for book in books:
    #     if book["Name"] == name:
    #         book["Read"] = True
    #         # book.update({"Name": name, "Author": book["Author"], "Read": True})
    # _save_all_books(books)


def delete_book(name: str) -> None:
    with DatabaseConnection('data.db') as connection:
        cursor = connection.cursor()
        cursor.execute("delete from  books  where name=?", (name,))

    # books = get_all_books()
    # for book in books: 
    #     books = [book for book in books if book['Name'] != name]
    # _save_all_books(books)
