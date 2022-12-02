from  ..  import pandas_tree

"""
Examples from: https://www.w3schools.com/sql/sql_where.asp

WHERE s_nationkey = 14    Equal, numeric
WHERE s_address = "Bread"   Equal, text
WHERE s_nationkey > 23    Greater than	
WHERE s_nationkey < 27.3 Less than	
WHERE s_nationkey >= -5   Greater than or equal	
WHERE s_nationkey <= 7823 Less than or equal
WHERE s_nationkey <> 17	Not equal, numeric
WHERE s_address <> "Simon"	Not equal, text

BETWEEN	Between a certain range	
WHERE s_nationkey BETWEEN 10 AND 20   Between a certain range, numeric
WHERE s_address BETWEEN 'And' AND 'But' Between a certain range, textual
WHERE s_nationkey NOT BETWEEN 19 AND 7    Not between a certain range

LIKE	Search for a pattern
In file: like_operator_tests.py

IN	To specify multiple possible values for a column
WHERE s_address IN ('Germany', 'France', 'UK')  Value in a set of values
WHERE s_address NOT IN ('Germany', 'France', 'UK')  Value not in a set of values
"""

def test_equal_numeric_value():
    # WHERE s_nationkey = 14    Equal, numeric
    
    target_string = 'supplier.s_nationkey = 14'
    in_string = "(supplier.s_nationkey = 14)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"

def test_equal_text_value():
    # WHERE s_address = 'Bread'   Equal, text
    
    target_string = "supplier.s_address = 'Bread'"
    in_string = "((supplier.s_address)::text = 'Bread'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_greater_than_value():
    # WHERE s_nationkey > 23    Greater than
    
    target_string = 'supplier.s_nationkey > 23'
    in_string = "(supplier.s_nationkey > 23)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"

def test_less_than_value():
    # WHERE s_nationkey < 27.3 Less than
    
    target_string = 'supplier.s_nationkey < 27.3'
    in_string = "((supplier.s_nationkey)::numeric < 27.3)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_greater_than_equal_to_value():
    # WHERE s_nationkey >= -5   Greater than or equal
    
    target_string = 'supplier.s_nationkey >= -5'
    in_string = "(supplier.s_nationkey >= '-5'::integer)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_less_than_equal_to_value():
    # WHERE s_nationkey <= 7823 Less than or equal
    
    target_string = 'supplier.s_nationkey <= 7823'
    in_string = "(supplier.s_nationkey <= 7823)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_not_equal_numeric_value():
    # WHERE s_nationkey <> 17	Not equal, numeric
    
    target_string = 'supplier.s_nationkey != 17'
    in_string = "(supplier.s_nationkey <> 17)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_not_equal_text_value():
    # WHERE s_address <> "Simon"	Not equal, text
    
    target_string = "supplier.s_address != 'Simon'"
    in_string = "((supplier.s_address)::text <> 'Simon'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_between_numeric():
    # WHERE s_nationkey BETWEEN 10 AND 20   Between a certain range, numeric
    
    target_string = "(supplier.s_nationkey >= 10) & (supplier.s_nationkey <= 20)"
    in_string = "((supplier.s_nationkey >= 10) AND (supplier.s_nationkey <= 20))"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"

def test_between_text():
    # WHERE s_address BETWEEN 'And' AND 'But' Between a certain range, textual
    
    target_string = "(supplier.s_address >= 'And') & (supplier.s_address <= 'But')"
    in_string = "(((supplier.s_address)::text >= 'And'::text) AND ((supplier.s_address)::text <= 'But'::text))"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"  
    
def test_not_between_numeric():
    # WHERE s_nationkey NOT BETWEEN 19 AND 7    Not between a certain range
    
    target_string = "(supplier.s_nationkey < 19) | (supplier.s_nationkey > 7)"
    in_string = "((supplier.s_nationkey < 19) OR (supplier.s_nationkey > 7))"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed" 
    
def test_in_values():
    # WHERE s_address IN ('Germany', 'France', 'UK')  Value in a set of values
    
    target_string = "supplier.s_address.isin(['Germany','France','UK'])"
    in_string = "((supplier.s_address)::text = ANY ('{Germany,France,UK}'::text[]))"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_in_single_values():
    # WHERE s_address IN ('Germany')  Value in a set of values
    
    target_string = "supplier.s_address = 'Germany'"
    in_string = "((supplier.s_address)::text = 'Germany'::text)"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"  

def test_not_in_values():
    # WHERE s_address NOT IN ('Germany', 'France', 'UK')  Value not in a set of values
    
    target_string = "~supplier.s_address.isin(['Germany','France','UK'])"
    in_string = "((supplier.s_address)::text <> ALL ('{Germany,France,UK}'::text[]))"
    out_string = pandas_tree.clean_filter_params(None, in_string)

    assert out_string == target_string, "Test Assertion Failed"  
