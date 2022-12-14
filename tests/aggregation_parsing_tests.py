import pytest
import os
import shutil
import subprocess
import inspect

import sql_to_pandas.pandas_tree as pandas_tree
import sql_to_pandas.pandas_tree_to_pandas as pandas_tree_to_pandas

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


TESTING_DIR = "../testing_outputs/"
QUERY_NAME = "query.sql"
CONVERTER_LOC = "sql_to_pandas/sql_to_pandas.py"
OUTPUT_NAME = "query.py"
RESULTS_LOC = "results"

# FUNCTIONS
def write_to_file(filepath, content):
    f = open(filepath, "w")
    f.write(content)
    f.close()
    
def read_from_file(filepath):
    f = open(filepath, "r")
    return f.read()

def remove_dir(folder):
    if os.path.exists(folder) and os.path.isdir(folder):
        shutil.rmtree(folder)
    
def delete_files_in_dir(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def count_files_in_directory(folder):
    _, _, files = next(os.walk(folder))
    return len(files)

def run_query(sql):
    # Write to a file
    write_to_file(TESTING_DIR+QUERY_NAME, sql)
        
    # Run this query
    cmd = ["python3", CONVERTER_LOC, '--file', TESTING_DIR+QUERY_NAME, "--output_location", TESTING_DIR, '--benchmarking', "False", "--name", OUTPUT_NAME]    
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception( f'Invalid result: { result.returncode }' )
    
    return str(read_from_file(TESTING_DIR+OUTPUT_NAME)).strip()

# Before all
@pytest.fixture(scope='module', autouse=True)
def setup():
    print("Run Setup")
    # Create directory for tests if doesn't already exist
    os.makedirs(TESTING_DIR, exist_ok=True)  # succeeds even if directory exists.
    
#  Before and after each
@pytest.fixture(autouse=True)
def run_around_tests():
    # Delete in Dir
    delete_files_in_dir(TESTING_DIR)
    # Code that will run before your test, for example:
    files_before = count_files_in_directory(TESTING_DIR)
    # A test function will be run at this point
    yield
    # Delete in Dir
    delete_files_in_dir(TESTING_DIR)
    # Cleanup, remove results from local
    remove_dir(RESULTS_LOC)
    # Code that will run after your test, for example:
    files_after = count_files_in_directory(TESTING_DIR)
    assert files_before == files_after

def test_aggregation_complete_complex():
    # Complex aggregation, from end to end (sql to pandas).
    # Expected pandas
    pandas_expected = inspect.cleandoc("""""").strip()
    
    sql_query = "SELECT ( COUNT ( o_custkey ) + AVG ( o_totalprice ) ) / ( SUM(o_orderkey) + MIN(o_shippriority) ) * 25 as fun_aggregate, ( MAX ( o_custkey ) * MIN ( o_totalprice ) ) * ( MAX(o_orderkey) - AVG(o_shippriority) ) - -5 as massive_query FROM orders;"
    
    pandas_query = run_query(sql_query)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_group_aggregation_complete_simple():
    # Tests of Group aggregation: Testing the same sort of stuff but after a group, See TPC-H Query 1
    # Expected pandas
    pandas_expected = inspect.cleandoc("""""").strip()
    
    sql_query = "SELECT o_custkey as customer, ( COUNT ( o_custkey ) + AVG ( o_totalprice ) ) / ( SUM(o_orderkey) + MIN(o_shippriority) ) * 25 as fun_aggregate, ( MAX ( o_custkey ) * MIN ( o_totalprice ) ) * ( MAX(o_orderkey) - AVG(o_shippriority) ) - -5 as massive_query FROM orders GROUP BY o_custkey;"
    
    pandas_query = run_query(sql_query)
    
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
    
    pandas_query = run_query(sql_query)
    
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
        remove_dir(TESTING_DIR)
    request.addfinalizer(remove_cleanup)
