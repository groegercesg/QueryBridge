import pytest
import os
import inspect


"""

Examples from: https://www.w3schools.com/sql/sql_count_avg_sum.asp

select min ( s_acctbal ) from supplier;
select max ( s_acctbal ) from supplier;
select count ( s_suppkey ) from supplier;
select avg ( s_acctbal ) from supplier;
select sum ( s_acctbal ) from supplier;

# Do tests with combinations of these

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

def test_min():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['mins_acctbal'] = [(df_filter_1.s_acctbal).min()]
        df_aggr_1 = df_aggr_1[['mins_acctbal']]
        return df_aggr_1""").strip()
    
    sql_query = "select min ( s_acctbal ) from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_min_alias():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['min_balance'] = [(df_filter_1.s_acctbal).min()]
        df_aggr_1 = df_aggr_1[['min_balance']]
        return df_aggr_1""").strip()
    
    sql_query = "select min ( s_acctbal ) as min_balance from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_max():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['maxs_acctbal'] = [(df_filter_1.s_acctbal).max()]
        df_aggr_1 = df_aggr_1[['maxs_acctbal']]
        return df_aggr_1""").strip()
    
    sql_query = "select max ( s_acctbal ) from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_max_alias():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['max_balance'] = [(df_filter_1.s_acctbal).max()]
        df_aggr_1 = df_aggr_1[['max_balance']]
        return df_aggr_1""").strip()
    
    sql_query = "select max ( s_acctbal ) as max_balance from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_count():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['counts_suppkey'] = [(df_filter_1.s_suppkey).count()]
        df_aggr_1 = df_aggr_1[['counts_suppkey']]
        return df_aggr_1""").strip()
    
    sql_query = "select count ( s_suppkey ) from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_count_alias():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['cnt_key'] = [(df_filter_1.s_suppkey).count()]
        df_aggr_1 = df_aggr_1[['cnt_key']]
        return df_aggr_1""").strip()
    
    sql_query = "select count ( s_suppkey ) as cnt_key from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_avg():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['avgs_acctbal'] = [(df_filter_1.s_acctbal).mean()]
        df_aggr_1 = df_aggr_1[['avgs_acctbal']]
        return df_aggr_1""").strip()
    
    sql_query = "select avg ( s_acctbal ) from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
    
def test_avg_alias():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['avg_balance'] = [(df_filter_1.s_acctbal).mean()]
        df_aggr_1 = df_aggr_1[['avg_balance']]
        return df_aggr_1""").strip()
    
    sql_query = "select avg ( s_acctbal ) as avg_balance from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_sum():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['sums_acctbal'] = [(df_filter_1.s_acctbal).sum()]
        df_aggr_1 = df_aggr_1[['sums_acctbal']]
        return df_aggr_1""").strip()
    
    sql_query = "select sum ( s_acctbal ) from supplier;"
    
    pandas_query = run_query(sql_query, constants)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected

def test_sum_alias():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['sum_balance'] = [(df_filter_1.s_acctbal).sum()]
        df_aggr_1 = df_aggr_1[['sum_balance']]
        return df_aggr_1""").strip()
    
    sql_query = "select sum ( s_acctbal ) as sum_balance from supplier;"
    
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
