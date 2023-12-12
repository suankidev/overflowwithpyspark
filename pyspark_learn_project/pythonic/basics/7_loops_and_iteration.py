nums = [x for x in range(1,6)]
print(nums,type(nums))


for num in nums:
    if num == 3:
        print("Found")
        break
    print(num)

for num in nums:
    if num == 3:
        print("Found")
        continue
    print(num)


print("="*10)

for num in nums:
    for letter in 'abc':
        print(num,letter)


for i in range(10):#range(1,11) by default starts with 0
    print(i)

x = 0
while x < 10:
    print(x)
    x+=1



