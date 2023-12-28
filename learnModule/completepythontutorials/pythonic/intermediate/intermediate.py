#%%

#list comrehension


nums = [1,2,3,4,5,6,7,8,9,10]

l =  list()

l = [x*x  for x in nums]
l = [x for x in nums if x%2 == 0]
print(l)

l = map(lambda x:x*x,nums)
l = filter(lambda x: x%2 == 0,nums)
print(l)

letter = "abcd"

#Q1. i want a (letter,num) pair for each letter

my_list = list()

for num in nums:
	for l in letter:
		my_list.append((num,l))



print(my_list)



my_list1 = [(a,b) for a in nums for b in letter]

print(my_list1)

#%%

#dictionay comprehentions

names = [ 'Bruce','Clark','Peter','Logan']
heros = ['Batman','Superman','Spiderman','Wolverine']



print(zip(names,heros))

#list of tuples
my_dict = {}
for i,j in zip(names,heros):
	print(i,j)
	my_dict[i]=j


print(my_dict)


#dict comprehension

my_dict = {name:hero for name,hero in zip(names,heros)}

print(my_dict)



# %%



