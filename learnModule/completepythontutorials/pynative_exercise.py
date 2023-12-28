# import RE module
import re

target_str = "My roll number is 25"
res = re.findall(r"\d", target_str)
# extract mathing value
print(res)


#%%

import re

target_string = "Jessa salary is 8000$"

str_pattern = r"\w"
pattern = re.compile(str_pattern)

res = pattern.match(target_string)

print(res.group())


res = re.search('\d', target_string)

print(res.group())

print(re.split('\s',target_string))

print(re.sub('\s','-',target_string))