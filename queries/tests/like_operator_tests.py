from  ..  import pandas_tree

"""
WHERE CustomerName LIKE 'a%'	Finds any values that start with "a"
WHERE CustomerName LIKE '%a'	Finds any values that end with "a"
WHERE CustomerName LIKE '%or%'	Finds any values that have "or" in any position
WHERE CustomerName LIKE '_r%'	Finds any values that have "r" in the second position
WHERE CustomerName LIKE 'a_%'	Finds any values that start with "a" and are at least 2 characters in length
WHERE CustomerName LIKE 'a__%'	Finds any values that start with "a" and are at least 3 characters in length
WHERE ContactName LIKE 'a%o'	Finds any values that start with "a" and ends with "o"
"""

def test_starting_value():
    target_string = "DogsAndCats"
    in_string = "((supplier.s_comment)::text ~~ '%Customer%Complaints%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
