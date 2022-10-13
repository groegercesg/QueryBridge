from pandas_list import clean_extra_brackets

str1 = "(sum(((l_extendedprice * ('1'::numeric - l_discount)) * ('1'::numeric + l_tax))))"

print(clean_extra_brackets(str1))

print("test")