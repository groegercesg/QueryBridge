import pytest
import os
import shutil
import subprocess
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

TESTING_DIR = "../testing_outputs/"
QUERY_NAME = "query.sql"
CONVERTER_LOC = "../sql_to_pandas.py"
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
    
def test_min():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['mins_acctbal'] = [(df_filter_1.s_acctbal).min()]
        df_aggr_1 = df_aggr_1[['mins_acctbal']]
        return df_aggr_1""").strip()
    
    sql_query = "select min ( s_acctbal ) from supplier;"
    
    pandas_query = run_query(sql_query)
    
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
    
    pandas_query = run_query(sql_query)
    
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
    
    pandas_query = run_query(sql_query)
    
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
    
    pandas_query = run_query(sql_query)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_count():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['counts_suppkey'] = [(df_filter_1.s_suppkey).count()]
        df_aggr_1 = df_aggr_1[['counts_suppkey']]
        return df_aggr_1""").strip()
    
    sql_query = "select count ( s_suppkey ) from supplier;"
    
    pandas_query = run_query(sql_query)
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_count_alias():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = supplier[['s_suppkey']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['cnt_key'] = [(df_filter_1.s_suppkey).count()]
        df_aggr_1 = df_aggr_1[['cnt_key']]
        return df_aggr_1""").strip()
    
    sql_query = "select count ( s_suppkey ) as cnt_key from supplier;"
    
    pandas_query = run_query(sql_query)
    
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
    
    pandas_query = run_query(sql_query)
    
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
    
    pandas_query = run_query(sql_query)
    
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
    
    pandas_query = run_query(sql_query)
    
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