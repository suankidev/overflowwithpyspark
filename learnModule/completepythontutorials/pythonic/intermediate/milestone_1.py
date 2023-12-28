MENU_PROMPT = "\nEnter 'a' to add a movie, 'l' to see you movies, 'f' to find a movie by title, or 'q' to quit:\n"

movies = []


def add_movie():
    title = input("enter the movie titile:")
    director = input("ente rthe movie director:")
    year = input("enter the movie release year:")
    movies.append({
        "title": title,
        "director": director,
        "year": year})


def show_movie(movies_list=movies):
    for i in movies_list:
        print_movie(i)


def print_movie(movie):
    print("title : {}".format(movie["title"]))
    print("directory : {}".format(movie["director"]))
    print("year : {}".format(movie["year"]))


def find_movies():
    find_by = input("filter by : year/title/director : ")
    looking_for = input("Enter the year/title/director name : ")
    found = find_by_attribute(movies, lambda x: x[find_by], looking_for)
    show_movie(found)


def find_by_attribute(items, finder, expected):
    found = []
    for movie in items:
        if finder(movie) == expected:
            found.append(movie)
    return found


map_user_input = {
    "a": add_movie,
    'l': show_movie,
    'f': find_movies
}


def menu():
    selection = input(MENU_PROMPT)
    while selection != "q":
        if selection in map_user_input:
            map_user_input[selection]()
        else:
            print("Uknown command-please try again.")
        selection = input(MENU_PROMPT)


if __name__ == "__main__":
    menu()
