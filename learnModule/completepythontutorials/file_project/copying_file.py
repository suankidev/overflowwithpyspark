#ask user for a list of 3 freinds
#for each friend , we'll tell the user whether  they are nearby
#for each nearby fiend, we'll savve their name  to nearby_friend.txt

#user_input=input("Enter list of 3 friend (,) seprated. ").split(',')

nearby_friend = ['Siva','Sujeet']
people_nearby_list=[]
print(nearby_friend)
people = open('people.txt','r')
near = [line.strip() for line in people.readlines()]

people.close()

print(near, type(near))
lambda x:x.replace('\n','')
print(people_nearby_list)
