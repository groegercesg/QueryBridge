str1 = "sum(l_extendedprice * ('1'::numeric - l_discount))"
str2 = "sum((l_extendedprice * ('1'::numeric - l_discount)) * ('1'::numeric + l_tax))"

from pandas_list import clean_type_information

print(clean_type_information("", str1))

print("Test")