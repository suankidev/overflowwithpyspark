from completepythontutorials.miltstone_project2.utils import database

USER_CHOICE = """
Enter:
- 'a' to add a new book
- 'l' to list all books
- 'r' to mark a book as read
- 'd' to delete a book
- 'q' to quit

Your choice: """


def menu():
    database.create_book_table()

    user_input = input(USER_CHOICE)
    while user_input != 'q':
        if user_input == 'a':
            prompt_add_book()
        elif user_input == 'l':
            list_books()
        elif user_input == 'r':
            prompt_read_book()
        elif user_input == 'd':
            prompt_delete_book()
        else:
            print("Unknown command. Please try again.")

        user_input = input(USER_CHOICE)


def prompt_add_book():
    book_name = input("Enter Book Name: ")
    book_author = input("Enter author Name: ")
    database.add_book(book_name, book_author)


def list_books():
    books = database.get_all_books()

    for book in books:
        read = 'YES' if book['Read'] == 1 else "No"
        print(f"{book['Name']} by {book['Author']}, read: {read}")



def prompt_read_book():
    book_name = input("Enter Book name: ")
    database.mark_book_as_read(book_name)


def prompt_delete_book():
    book_name = input("Enter Book Name: ")
    database.delete_book(book_name)


menu()
