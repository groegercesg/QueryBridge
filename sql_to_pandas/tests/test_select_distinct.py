import pytest
import os
import inspect

"""
Examples from: https://www.w3schools.com/sql/sql_distinct.asp

select o_custkey from orders limit 10;
select count( o_custkey ) from orders;
select distinct ( o_custkey ) from orders limit 10;
select count( distinct ( o_custkey ) ) from orders;
"""

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
    
    yield
    
    # Delete in Dir
    for dir in [constants.INPUTS_DIR, constants.OUTPUTS_DIR]:
        delete_files_in_dir(dir)
    # Code that will run after your test, for example:
    files_after_input = count_files_in_directory(constants.INPUTS_DIR)
    files_after_output = count_files_in_directory(constants.OUTPUTS_DIR)
    
    assert files_before_input == files_after_input
    assert files_before_output == files_after_output

def test_standard_select():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_custkey']]
        df_limit_1 = df_filter_1[['o_custkey']]
        df_limit_1 = df_limit_1.head(10)
        return df_limit_1""").strip()
    
    # A standard select, let's just as a baseline check this works
    sql_query = "select o_custkey from orders limit 10;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_simple_count():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['counto_custkey'] = [(df_filter_1.o_custkey).count()]
        df_aggr_1 = df_aggr_1[['counto_custkey']]
        return df_aggr_1""").strip()
    
    # A count select select
    sql_query = "select count( o_custkey ) from orders;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_simple_distinct():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_custkey']]
        df_unique_1 = pd.DataFrame()
        df_unique_1['o_custkey'] = df_filter_1['o_custkey'].unique()
        df_unique_1 = df_unique_1.sort_values(by=['o_custkey'], ascending=[True])
        df_limit_1 = df_unique_1[['o_custkey']]
        df_limit_1 = df_limit_1.head(10)
        return df_limit_1""").strip()
    
    # A count select select
    sql_query = "select distinct ( o_custkey ) from orders limit 10;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_count_distinct():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['countdistincto_custkey'] = [len((df_filter_1.o_custkey).unique())]
        df_aggr_1 = df_aggr_1[['countdistincto_custkey']]
        return df_aggr_1""").strip()
    
    # A count select select
    sql_query = "select count( distinct ( o_custkey ) ) from orders;"
    
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
    