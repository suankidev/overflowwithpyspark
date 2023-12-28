friends = ["Sujeet", "Siva", "Rama", "Tathcharya", "Gundappa", "govind", "Krishndevroy", "Raka"]

friends_with_lower = map(lambda x: x.lower(), friends)

print(next(friends_with_lower))


class User:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    @classmethod
    def from_dict(cls, data):
        return cls(data['username'], data['password'])


users = [{'username': 'Sujeet', 'password': 123},
         {'username': 'Siva', 'password': 1234}
         ]

users_one = [User.from_dict(user) for user in users]

users_two = map(User.from_dict, users)

friends = [

    {"Name": "Sujeet", "location": "Gorakhpur"},
    {"Name": "Ramesh", "location": "Gorakhpur"},
    {"Name": "Siva", "location": "Andhra Pradesh"},
    {"Name": "Sahil", "location": "Solapur"}
]

your_locaion = input("Enter your location ")

friend_nearby = [f for f in friends if f['location'] == your_locaion]

if len(friend_nearby) > 0:
    print("You are not alone ")

if any(friend_nearby):
    print("You are not alone")

"""
any --> try to evaluate to true ,if any value is true then it evaluate to true.
all --> all of them should be true in list/values
What evaluate to False ?
0, 0,0
NOne
[], (), {}
False
"""

print(all([1, 2, 3, 4, 5]))  # True
print(all([1, 2, 3, 4, 5, 0]))  # False
