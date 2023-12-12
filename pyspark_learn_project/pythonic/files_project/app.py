#Ask the user for a list of 3 friends
#for each friend we'll tell the user whether tey are nearby
#for each nearby friend , we'll save their name to nearby_friend.txt

#user_input = input("Enter the list of 3 friends seperated  by commas(no space please): ").split(",")
user_input = "Sujeet,Tiger"

with open("friends.txt","r") as reader:
    list_of_friends = [line.strip() for line in reader]  #strip() to remove new line and spaces
    user_input = set(user_input)
    print(user_input)
    people_nearby = set(list_of_friends)
    friend_nearby = user_input.intersection(people_nearby)

with open("friend_nearby.txt","w+") as writer:
    for line in friend_nearby:
        writer.write(line+"\n")



with open("addresses.csv","r") as csv_reader:
    lines = csv_reader.readlines()

lines = [line.strip() for line in lines[1::]]#ignore the first line

for row in lines:
    final_row = row.split(",")
    first_name = final_row[0]
    last_name = final_row[1]
    address = final_row[2::]

    full_name = first_name + last_name
    prnt_line = f"Full Name : {full_name.title()} and Address: {''.join(address).capitalize()}"
    print(prnt_line)


test_str ="this is test"

print(test_str.casefold())

