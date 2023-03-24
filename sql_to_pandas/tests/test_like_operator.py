from .context import pandas_tree as pandas_tree
from .context import pandas_tree_to_pandas as pandas_tree_to_pandas

"""
Examples from: https://www.w3schools.com/sql/sql_like.asp

WHERE s_comment LIKE 'a%'	Finds any values that start with "a"
WHERE s_comment LIKE '%a'	Finds any values that end with "a"
WHERE s_comment LIKE '%or%'	Finds any values that have "or" in any position
WHERE s_comment LIKE '_r%'	Finds any values that have "r" in the second position
WHERE s_comment LIKE 'a_%'	Finds any values that start with "a" and are at least 2 characters in length
WHERE s_comment LIKE 'a__%'	Finds any values that start with "a" and are at least 3 characters in length
WHERE s_comment LIKE 'a%o'	Finds any values that start with "a" and ends with "o"
"""

codeCompHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)

def test_starting_value():
    # WHERE s_comment LIKE 'a%'	 - Finds any values that start with "a"
    
    target_string = '(supplier.s_comment.str.contains("^a.*?$",regex=True))'
    in_string = "((supplier.s_comment)::text ~~ 'a%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_ending_value():
    # WHERE s_comment LIKE '%a'	Finds any values that end with "a"
    
    target_string = '(supplier.s_comment.str.contains("^.*?a$",regex=True))'
    in_string = "((supplier.s_comment)::text ~~ '%a'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"

    
def test_not_ending_value():
    # WHERE s_comment NOT LIKE '%a'	Finds any values that don't end with "a"
    
    target_string = '(supplier.s_comment.str.contains("^.*?a$",regex=True) == False)'
    in_string = "((supplier.s_comment)::text !~~ '%a'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_any_postition_value():
    # WHERE s_comment LIKE '%or%'	Finds any values that have "or" in any position
    
    target_string = '(supplier.s_comment.str.contains("^.*?or.*?$",regex=True))'
    in_string = "((supplier.s_comment)::text ~~ '%or%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"

def test_second_position():
    # WHERE s_comment LIKE '_r%'	Finds any values that have "r" in the second position
    
    target_string = '(supplier.s_comment.str.contains("^.r.*?$",regex=True))'
    in_string = "((supplier.s_comment)::text ~~ '_r%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_not_second_position():
    # WHERE s_comment NOT LIKE '_r%'	Finds any values that don't have "r" in the second position
    
    target_string = '(supplier.s_comment.str.contains("^.r.*?$",regex=True) == False)'
    in_string = "((supplier.s_comment)::text !~~ '_r%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_starting_value_two_chars():
    # WHERE s_comment LIKE 'a_%'	Finds any values that start with "a" and are at least 2 characters in length

    target_string = '(supplier.s_comment.str.contains("^a..*?$",regex=True))'
    in_string = "((supplier.s_comment)::text ~~ 'a_%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_starting_value_three_chars():
    # WHERE s_comment LIKE 'a__%'	Finds any values that start with "a" and are at least 3 characters in length

    target_string = '(supplier.s_comment.str.contains("^a...*?$",regex=True))'
    in_string = "((supplier.s_comment)::text ~~ 'a__%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_starting_and_ending_value():
    # WHERE s_comment LIKE 'a%o'	Finds any values that start with "a" and ends with "o"

    target_string = '(supplier.s_comment.str.contains("^a.*?o$",regex=True))'
    in_string = "((supplier.s_comment)::text ~~ 'a%o'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_contains_value():
    # WHERE s_comment LIKE '%horse%' Finds any values that contain the string "horse"
    
    target_string = '(supplier.s_comment.str.contains("^.*?horse.*?$",regex=True))'
    in_string = "((supplier.s_comment)::text ~~ '%horse%'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_quotes_value():
    # WHERE p_type LIKE '%BRASS'
    
    target_string = '(part.p_type.str.contains("^.*?BRASS$",regex=True)) & (part.p_size == 15)'
    in_string = "(((part.p_type)::text ~~ '%BRASS'::text) AND (part.p_size = 15))"
    out_string = pandas_tree.clean_filter_params(None, in_string, codeCompHelper)

    assert out_string == target_string, "Test Assertion Failed"
