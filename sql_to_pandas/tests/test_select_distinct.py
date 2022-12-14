import pytest
import os
import shutil
import subprocess
import inspect

"""
Examples from: https://www.w3schools.com/sql/sql_distinct.asp

select o_custkey from orders limit 10;
select count( o_custkey ) from orders;
select distinct ( o_custkey ) from orders limit 10;
select count( distinct ( o_custkey ) ) from orders;
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

def test_standard_select():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_custkey']]
        df_limit_1 = df_filter_1[['o_custkey']]
        result = df_limit_1.head(10)
        return result""").strip()
    
    # A standard select, let's just as a baseline check this works
    sql_query = "select o_custkey from orders limit 10;"
    
    # Write to a file
    write_to_file(TESTING_DIR+QUERY_NAME, sql_query)
        
    # Run this query
    cmd = ["python3", CONVERTER_LOC, '--file', TESTING_DIR+QUERY_NAME, "--output_location", TESTING_DIR, '--benchmarking', "False", "--name", OUTPUT_NAME]    
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception( f'Invalid result: { result.returncode }' )
    
    pandas_query = str(read_from_file(TESTING_DIR+OUTPUT_NAME)).strip()
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_simple_count():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_custkey']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['counto_custkey'] = [(o_custkey).count()]
        df_aggr_1 = df_aggr_1[['counto_custkey']]""").strip()
    
    # A count select select
    sql_query = "select count( o_custkey ) from orders;"
    
    # Write to a file
    write_to_file(TESTING_DIR+QUERY_NAME, sql_query)
        
    # Run this query
    cmd = ["python3", CONVERTER_LOC, '--file', TESTING_DIR+QUERY_NAME, "--output_location", TESTING_DIR, '--benchmarking', "False", "--name", OUTPUT_NAME]    
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception( f'Invalid result: { result.returncode }' )
    
    pandas_query = str(read_from_file(TESTING_DIR+OUTPUT_NAME)).strip()
    
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
        result = df_limit_1.head(10)
        return result""").strip()
    
    # A count select select
    sql_query = "select distinct ( o_custkey ) from orders limit 10;"
    
    # Write to a file
    write_to_file(TESTING_DIR+QUERY_NAME, sql_query)
        
    # Run this query
    cmd = ["python3", CONVERTER_LOC, '--file', TESTING_DIR+QUERY_NAME, "--output_location", TESTING_DIR, '--benchmarking', "False", "--name", OUTPUT_NAME]    
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception( f'Invalid result: { result.returncode }' )
    
    pandas_query = str(read_from_file(TESTING_DIR+OUTPUT_NAME)).strip()
    
    print("Pandas Query:")
    print(pandas_query)
    print("Pandas Expected:")
    print(pandas_expected)
    
    assert pandas_query == pandas_expected
    
def test_count_distinct():
    # Expected pandas
    pandas_expected = inspect.cleandoc("""
        df_filter_1 = orders[['o_custkey']]
        df_aggr_1 = pd.DataFrame()
        df_aggr_1['countDISTINCTo_custkey'] = [(df_filter_1['o_custkey']).nunique()]
        df_aggr_1 = df_aggr_1[['countDISTINCTo_custkey']]""").strip()
    
    # A count select select
    sql_query = "select count( distinct ( o_custkey ) ) from orders;"
    
    # Write to a file
    write_to_file(TESTING_DIR+QUERY_NAME, sql_query)
        
    # Run this query
    cmd = ["python3", CONVERTER_LOC, '--file', TESTING_DIR+QUERY_NAME, "--output_location", TESTING_DIR, '--benchmarking', "False", "--name", OUTPUT_NAME]    
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception( f'Invalid result: { result.returncode }' )
    
    pandas_query = str(read_from_file(TESTING_DIR+OUTPUT_NAME)).strip()
    
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
    