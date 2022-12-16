import pytest
import os
import shutil
import subprocess
import inspect

from .context import pandas_tree as pandas_tree
from .context import pandas_tree_to_pandas as pandas_tree_to_pandas

"""
Tests of parsing aggregations:
    - Nested tests, direct to the function (do_aggregation)
    - A sum of an avg
        - ERROR:  aggregate function calls cannot be nested
    - Count of a min of an avg of a max of a sum
        - ERROR:  aggregate function calls cannot be nested
    - A sum divided by an average
        - With and withount aliasees
    - A sum multiplied by a count
        - With and withount aliasees
    - A min minuses a max
        - With and withount aliasees
    - A count plus an average all divided a two columns added and multiplied by 25
        - With and withount aliasees

Tests of the SQL to pandas flow
    - Featuring these complex aggregations
    - Two or Three of them
    
Tests of Group aggregation:
    - Testing the same sort of stuff but after a group
    - See TPC-H Query 1
    
Reformed Aggregation Implementation Notes:
    - One function for both Aggregation and Group Aggregation, write first for Aggregation
    - Use (Binary) Expression Trees
        - With all the standard operators (+, -, *, /) and special ones like Avg, Sum, Min, Max, Count
    - Build a tree for a Expression, visualise it, then evaluate it by converting it to Python
        - We can have a built-in knowledge for what each operator is represented like in Python.

"""

class content():
    def __init__(self, output):
        self.output = output

def test_parse_agg_simple():    
    target_string = ["CURRENT_DF['sumo_totalprice'] = [(PREV_DF.o_totalprice).sum()]"]
    in_string = ["sum(o_totalprice)"]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_simple():
    # A sum divided by an average
    target_string = ["CURRENT_DF['sumo_totalpriceavgo_custkey'] = [((PREV_DF.o_totalprice).sum() / (PREV_DF.o_custkey).mean())]"]
    in_string = ["sum(o_totalprice) / avg(o_custkey)"]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_simple_alias():
    # A sum divided by an average, with an alias
    target_string = ["CURRENT_DF['fun_aggregate'] = [((PREV_DF.o_totalprice).sum() / (PREV_DF.o_custkey).mean())]"]
    in_string = [("sum(o_totalprice) / avg(o_custkey)", "fun_aggregate")]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_alternate():
    # A sum multiplied by a count
    target_string = ["CURRENT_DF['sumo_totalpricecounto_custkey'] = [((PREV_DF.o_totalprice).sum() * (PREV_DF.o_custkey).count())]"]
    in_string = ["sum(o_totalprice) * count(o_custkey)"]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_alternate_alias():
    # A sum multiplied by a count, with an alias
    target_string = ["CURRENT_DF['multiply'] = [((PREV_DF.o_totalprice).sum() * (PREV_DF.o_custkey).count())]"]
    in_string = [("sum(o_totalprice) * count(o_custkey)", "multiply")]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_minus():
    # A min minuses a max
    target_string = ["CURRENT_DF['mino_totalpricemaxo_totalprice'] = [((PREV_DF.o_totalprice).min() - (PREV_DF.o_totalprice).max())]"]
    in_string = ["min(o_totalprice) - max(o_totalprice)"]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_minus_complex():
    # A min minuses a max
    target_string = ["CURRENT_DF['mino_totalpricemaxo_totalprice17'] = [((PREV_DF.o_totalprice).min() / ((PREV_DF.o_totalprice).max() * -17))]"]
    in_string = ["min(o_totalprice) / (max(o_totalprice) * -17)"]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_minus_alias():
    # A min minuses a max, with an alias
    target_string = ["CURRENT_DF['minus_agg'] = [((PREV_DF.o_totalprice).min() - (PREV_DF.o_totalprice).max())]"]
    in_string = [("min(o_totalprice) - max(o_totalprice)", "minus_agg")]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_complex():
    # A count plus an average all divided a two columns added and multiplied by 25
    target_string = ["CURRENT_DF['counto_custkeyavgo_totalpricesumo_orderkeymino_shippriority25'] = [((((PREV_DF.o_custkey).count() + (PREV_DF.o_totalprice).mean()) / ((PREV_DF.o_orderkey).sum() + (PREV_DF.o_shippriority).min())) * 25)]"]
    in_string = ["(count(o_custkey) + avg(o_totalprice)) / (sum(o_orderkey) + min(o_shippriority)) * 25"]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"
    
def test_parse_agg_nest_complex_alias():
    # A count plus an average all divided a two columns added and multiplied by 25
    target_string = ["CURRENT_DF['complex_aggregation'] = [((((PREV_DF.o_custkey).count() + (PREV_DF.o_totalprice).mean()) / ((PREV_DF.o_orderkey).sum() + (PREV_DF.o_shippriority).min())) * 25)]"]
    in_string = [("(count(o_custkey) + avg(o_totalprice)) / (sum(o_orderkey) + min(o_shippriority)) * 25", "complex_aggregation")]
    in_class = content(in_string)
    ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
    tHelper = pandas_tree_to_pandas.TreeHelper("", True)
    out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper, tHelper)
    
    print("Out String:")
    print(out_string)
    print("Target String:")
    print(target_string)

    assert out_string == target_string, "Test Assertion Failed"

# Testing methods
from general_test_functions import *

# Variable Names
constants = Constants_class("testing_inputs/", "testing_outputs/", "query.sql", "../sql_to_pandas.py", "query.py")

# Before all
@pytest.fixture(scope='module', autouse=True)
def setup():
    print("Run Setup")
    # Create directory for tests if doesn't already exist
    for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
        os.makedirs(dir, exist_ok=True)  # succeeds even if directory exists.
    
#  Before and after each
@pytest.fixture(autouse=True)
def run_around_tests():
    # Delete in Dir
    for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
        delete_files_in_dir(dir)
    # Code that will run before your test, for example:
    files_before_input = count_files_in_directory(constants.INPUTS_DIR)
    files_before_output = count_files_in_directory(constants.OUTPUTS_DIR)
    
    # A test function will be run at this point
    yield
    
    # Delete in Dir
    for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
        delete_files_in_dir(dir)
    # Code that will run after your test, for example:
    files_after_input = count_files_in_directory(constants.INPUTS_DIR)
    files_after_output = count_files_in_directory(constants.OUTPUTS_DIR)
    
    assert files_before_input == files_after_input
    assert files_before_output == files_after_output

def test_group_aggregation_complete_basic():
    # Tests of Group aggregation: Testing the same sort of stuff but after a group, See TPC-H Query 1
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
        df_filter_1['before_1'] = (df_filter_1.o_orderkey)
        df_group_1 = df_filter_1 \\
            .groupby(['o_custkey']) \\
            .agg(
                count_before_1=("before_1", "count"),
            )
        df_group_1['count_orders'] = df_group_1.count_before_1
        df_group_1 = df_group_1[['count_orders']]
        return df_group_1""").strip()
    
    sql_query = "SELECT COUNT ( o_orderkey ) as count_orders FROM orders GROUP BY o_custkey;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected

def test_aggregation_complete_complex():
    # Complex aggregation, from end to end (sql to pandas).
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['fun_aggregate'] = [((((df_filter_1.o_custkey).count() + (df_filter_1.o_totalprice).mean()) / ((df_filter_1.o_orderkey).sum() + (df_filter_1.o_shippriority).min())) * 25)]
        df_aggr_1['massive_query'] = [((((df_filter_1.o_custkey).max() * (df_filter_1.o_totalprice).min()) * ((df_filter_1.o_orderkey).max() - (df_filter_1.o_shippriority).mean())) + 5)]
        df_aggr_1 = df_aggr_1[['fun_aggregate', 'massive_query']]
        return df_aggr_1""").strip()
    
    sql_query = "SELECT ( COUNT ( o_custkey ) + AVG ( o_totalprice ) ) / ( SUM(o_orderkey) + MIN(o_shippriority) ) * 25 as fun_aggregate, ( MAX ( o_custkey ) * MIN ( o_totalprice ) ) * ( MAX(o_orderkey) - AVG(o_shippriority) ) - -5 as massive_query FROM orders;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_group_aggregation_complete_simple():
    # Tests of Group aggregation: Testing the same sort of stuff but after a group, See TPC-H Query 1
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
        df_filter_1['before_1'] = (df_filter_1.o_custkey)
        df_filter_1['before_2'] = (df_filter_1.o_totalprice)
        df_filter_1['before_3'] = (df_filter_1.o_orderkey)
        df_filter_1['before_4'] = (df_filter_1.o_shippriority)
        df_filter_1['before_5'] = (df_filter_1.o_custkey)
        df_filter_1['before_6'] = (df_filter_1.o_totalprice)
        df_filter_1['before_7'] = (df_filter_1.o_orderkey)
        df_filter_1['before_8'] = (df_filter_1.o_shippriority)
        df_group_1 = df_filter_1 \\
            .groupby(['o_custkey']) \\
            .agg(
                count_before_1=("before_1", "count"),
                mean_before_2=("before_2", "mean"),
                sum_before_3=("before_3", "sum"),
                min_before_4=("before_4", "min"),
                max_before_5=("before_5", "max"),
                min_before_6=("before_6", "min"),
                max_before_7=("before_7", "max"),
                mean_before_8=("before_8", "mean"),
            )
        df_group_1['customer'] = (df_filter_1.o_custkey)
        df_group_1['fun_aggregate'] = (((df_group_1.count_before_1 + df_group_1.mean_before_2) / (df_group_1.sum_before_3 + df_group_1.min_before_4)) * 25)
        df_group_1['massive_query'] = (((df_group_1.max_before_5 * df_group_1.min_before_6) * (df_group_1.max_before_7 - df_group_1.mean_before_8)) + 5)
        df_group_1 = df_group_1[['customer', 'fun_aggregate', 'massive_query']]
        return df_group_1""").strip()
    
    sql_query = "SELECT o_custkey as customer, ( COUNT ( o_custkey ) + AVG ( o_totalprice ) ) / ( SUM(o_orderkey) + MIN(o_shippriority) ) * 25 as fun_aggregate, ( MAX ( o_custkey ) * MIN ( o_totalprice ) ) * ( MAX(o_orderkey) - AVG(o_shippriority) ) - -5 as massive_query FROM orders GROUP BY o_custkey;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_group_aggregation_complete_annoying():
    # Tests of Group aggregation: Testing the same sort of stuff but after a group, See TPC-H Query 1
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
        df_filter_1['before'] = (df_filter_1.o_custkey * (df_filter_1.o_totalprice / -1))
        df_group_1 = df_filter_1 \
            .groupby(['o_custkey']) \
            .agg(
                count_before=("before", "count"),
                mean_o_totalprice=("o_totalprice", "mean"),
            )
        df_group_1['?content?'] = (df_group_1.count_before + df_group_1.mean_o_totalprice)
        df_group_1 = df_group_1[['?content?']]
        return df_group_1""").strip()
    
    sql_query = "SELECT (COUNT ( o_custkey * (o_totalprice / -1) ) + AVG ( o_totalprice )) FROM orders GROUP BY o_custkey;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_group_aggregation_complete_complex():
    # Tests of Group aggregation: Testing the same sort of stuff but after a group, See TPC-H Query 1
    # Expected pandas
    pandas_expected = inspect.cleandoc("""""").strip()
    
    sql_query = "select l_returnflag, l_linestatus, sum(l_quantity) * avg(l_discount * 0.5) - 72 as sum_qty, sum(l_extendedprice) / ((count(l_quantity) - min(l_discount) - min(l_tax)) * 0.5) as sum_base_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge from lineitem group by l_returnflag, l_linestatus;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
   
# After all
@pytest.fixture(scope="module", autouse=True)
def cleanup(request):
    """Cleanup a testing directory once we are finished."""
    def remove_cleanup():
        for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
            remove_dir(dir)
    request.addfinalizer(remove_cleanup)
