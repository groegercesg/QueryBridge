from  ..  import pandas_tree

"""
Examples from: https://www.w3schools.com/sql/sql_like.asp

WHERE CustomerName LIKE 'a%'	Finds any values that start with "a"
WHERE CustomerName LIKE '%a'	Finds any values that end with "a"
WHERE CustomerName LIKE '%or%'	Finds any values that have "or" in any position
WHERE CustomerName LIKE '_r%'	Finds any values that have "r" in the second position
WHERE CustomerName LIKE 'a_%'	Finds any values that start with "a" and are at least 2 characters in length
WHERE CustomerName LIKE 'a__%'	Finds any values that start with "a" and are at least 3 characters in length
WHERE ContactName LIKE 'a%o'	Finds any values that start with "a" and ends with "o"
"""

def test_starting_value():
    # WHERE s_comment LIKE 'a%'	 - Finds any values that start with "a"
    
    target_string = 'supplier.s_comment.str.startswith("a")'
    in_string = "((supplier.s_comment)::text ~~ 'a%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_ending_value():
    # WHERE s_comment LIKE '%a'	Finds any values that end with "a"
    
    target_string = 'supplier.s_comment.str.endswith("a")'
    in_string = "((supplier.s_comment)::text ~~ '%a'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"

    
def test_not_ending_value():
    # WHERE s_comment NOT LIKE '%a'	Finds any values that don't end with "a"
    
    target_string = 'supplier.s_comment.str.endswith("a")'
    in_string = "((supplier.s_comment)::text !~~ '%a'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_any_postition_value():
    # WHERE s_comment LIKE '%or%'	Finds any values that have "or" in any position
    
    target_string = 'supplier.s_comment.str.contains("or")'
    in_string = "((supplier.s_comment)::text ~~ '%or%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"

def test_second_position():
    # WHERE s_comment LIKE '_r%'	Finds any values that have "r" in the second position
    
    target_string = 'supplier.s_comment.str[1].contains("r")'
    in_string = "((supplier.s_comment)::text ~~ '_r%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_not_second_position():
    # WHERE s_comment NOT LIKE '_r%'	Finds any values that don't have "r" in the second position
    
    target_string = 'supplier.s_comment.str[1].contains("r")'
    in_string = "((supplier.s_comment)::text !~~ '_r%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_starting_value_two_chars():
    # WHERE s_comment LIKE 'a_%'	Finds any values that start with "a" and are at least 2 characters in length

    target_string = 'supplier.s_comment.str[1].contains("r")'
    in_string = "((supplier.s_comment)::text ~~ 'a_%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_starting_value_three_chars():
    # WHERE s_comment LIKE 'a__%'	Finds any values that start with "a" and are at least 3 characters in length

    target_string = 'supplier.s_comment.str[1].contains("r")'
    in_string = "((supplier.s_comment)::text ~~ 'a__%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_starting_and_ending_value():
    # WHERE ContactName LIKE 'a%o'	Finds any values that start with "a" and ends with "o"

    target_string = 'supplier.s_comment.str[1].contains("r")'
    in_string = "((supplier.s_comment)::text ~~ 'a%o'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
