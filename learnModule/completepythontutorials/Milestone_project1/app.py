MENU_POMPT = "\"Enter 'a' to add a movie, '1' to see your movie, 'f' to find a movie by title, or 'q' to quit:"

movies = []


# you may want to create a function for this code


def add_movie():
    title = input("Enter the movie title: ")
    director = input("Enter the movie director: ")
    year = input("Enter the movie year: ")

    movies.append({
        "title": title,
        "director": director,
        "year": year
    })


def print_movie(movie):
    print(f" Title: {movie['title']}")
    print(f" Directory: {movie['director']}")
    print(f" Release year {movie['year']}")


def show_movies():
    for movie in movies:
        print_movie(movie)


def find_movie():
    search_title = input("Enter movie title you'r looking for: ")

    for movie in movies:
        if movie["title"] == search_title:
            print_movie(movie)


operation = {"a": add_movie,
             "l": show_movies,
             "f": find_movie
             }


# And another function here for the user menu

def menu():
    selection = input(MENU_POMPT)
    while selection != 'q':
        if selection in operation:
            action = operation.get(selection)
            action()
        else:
            print("Enter valid option: ")
        selection = input(MENU_POMPT)

menu()
