import shutil
import os
import subprocess
import random, string
import json

def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)

def add_line_at_end(filename, line):
    with open(filename, "a") as f:
        f.write('\n' + line.rstrip('\r\n'))
        
def execute_permission_it(full_path):
    # Permissions it
    chmod_command = f"chmod u+x {full_path}"
    process = subprocess.Popen(chmod_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
        
def setup_sdqlpy(sdqlpy_setup):
    # Setup SDQLpy
    location = sdqlpy_setup["Location"]
    
    # Make folder (and delete if exists)
    if os.path.exists(location) and os.path.isdir(location):
        shutil.rmtree(location)
    os.makedirs(location)
    
    # Copy install script to location
    _, install_base_path = os.path.split(sdqlpy_setup["Install Script"])
    install_full_path = f"{location}/{install_base_path}"
    shutil.copyfile(sdqlpy_setup["Install Script"], install_full_path)
    
    execute_permission_it(install_full_path)
    
    # Change directory
    previous_working_directory = os.getcwd()
    os.chdir(location)
    
    result = subprocess.run(f"./{install_base_path}", stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception("Error when Setting up SDQLpy")
    
    # Delete it
    os.chdir(previous_working_directory)
    os.remove(install_full_path)
    
    # Create queries directory
    queries_path = f"{sdqlpy_setup['Location']}/{sdqlpy_setup['Queries Directory']}"
    os.mkdir(queries_path)
    
    # Move the required files in
    # Bench Run
    _, bench_run_base = os.path.split(sdqlpy_setup['Bench Run'])
    new_bench_run_path = f"{sdqlpy_setup['Location']}/{bench_run_base}"
    shutil.copyfile(sdqlpy_setup["Bench Run"], new_bench_run_path)
    
    execute_permission_it(new_bench_run_path)
    
    # Bench Class
    _, bench_class_base = os.path.split(sdqlpy_setup['Bench Class'])
    new_bench_class_path = f"{sdqlpy_setup['Location']}/{sdqlpy_setup['Queries Directory']}/{bench_class_base}"
    shutil.copyfile(sdqlpy_setup["Bench Class"], new_bench_class_path)

def teardown_sdqlpy(sdqlpy_location):
    # Teardown SDQLpy, delete if exists
    shutil.rmtree(sdqlpy_location)
    
TPCH_TABLES = [
    "lineitem", "customer", "orders", "nation", 
    "region", "part", "partsupp", "supplier"
]
    
def build_sdqlpy_info(data_location: str, desired_tables: list, number_of_threads: int) -> str:
    if not all([x in TPCH_TABLES for x in desired_tables]):
        raise Exception("Unrecognised tables in query")
    
    tpch_table_types = {
        "lineitem": 'lineitem_type = {record({"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float, "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(1), "l_linestatus": string(1), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date, "l_shipinstruct": string(25), "l_shipmode": string(10), "l_comment": string(44), "l_NA": string(1)}): bool}',
        "customer": 'customer_type = {record({"c_custkey": int, "c_name": string(25), "c_address": string(40), "c_nationkey": int, "c_phone": string(15), "c_acctbal": float, "c_mktsegment": string(10), "c_comment": string(117), "c_NA": string(1)}): bool}',
        "orders": 'orders_type = {record({"o_orderkey": int, "o_custkey": int, "o_orderstatus": string(1), "o_totalprice": float, "o_orderdate": date, "o_orderpriority": string(15), "o_clerk": string(15), "o_shippriority": int, "o_comment": string(79), "o_NA": string(1)}): bool}',
        "nation": 'nation_type = {record({"n_nationkey": int, "n_name": string(25), "n_regionkey": int, "n_comment": string(152), "n_NA": string(1)}): bool}',
        "region": 'region_type = {record({"r_regionkey": int, "r_name": string(25), "r_comment": string(152), "r_NA": string(1)}): bool}',
        "part": 'part_type = {record({"p_partkey": int, "p_name": string(55), "p_mfgr": string(25), "p_brand": string(10), "p_type": string(25), "p_size": int, "p_container": string(10), "p_retailprice": float, "p_comment": string(23), "p_NA": string(1)}): bool}',
        "partsupp": 'partsupp_type = {record({"ps_partkey": int, "ps_suppkey": int, "ps_availqty": float, "ps_supplycost": float, "ps_comment": string(199), "ps_NA": string(1)}): bool}',
        "supplier": 'supplier_type = {record({"s_suppkey": int, "s_name": string(25), "s_address": string(40), "s_nationkey": int, "s_phone": string(15), "s_acctbal": float, "s_comment": string(101), "s_NA": string(1)}): bool}'
    }
    
    tpch_table_definitions = {
        "lineitem": 'lineitem = read_csv(dataset_path + "lineitem.tbl.csv", lineitem_type, "lineitem")',
        "customer": 'customer = read_csv(dataset_path + "customer.tbl.csv", customer_type, "customer")',
        "orders": 'orders = read_csv(dataset_path + "orders.tbl.csv", orders_type, "orders")',
        "nation": 'nation = read_csv(dataset_path + "nation.tbl.csv", nation_type, "nation")',
        "region": 'region = read_csv(dataset_path + "region.tbl.csv", region_type, "region")',
        "part": 'part = read_csv(dataset_path + "part.tbl.csv", part_type, "part")',
        "partsupp": 'partsupp = read_csv(dataset_path + "partsupp.tbl.csv", partsupp_type, "partsupp")',
        "supplier": 'supplier = read_csv(dataset_path + "supplier.tbl.csv", supplier_type, "supplier")'
    }
    
    full_data_location = '"' + str(os.getcwd()) + str("/") + str(data_location) + str("/") + '"'
    
    new_sdqlpy_info = """from sdqlpy.sdql_lib import *
from sdqlpy_benchmark_runner import bench_runner

print("Starting to Load Data")

dataset_path = """ + full_data_location + """

"""
    for table in desired_tables:
        new_sdqlpy_info += f"{tpch_table_types[table]}\n"

    new_sdqlpy_info += "\n"
    
    for table in desired_tables:
        new_sdqlpy_info += f"{tpch_table_definitions[table]}\n"
        
    # This 'sdqlpy_init(...)' controls whether it's single threaded or not
    new_sdqlpy_info += """print("Data Loaded")

sdqlpy_init(1, """ + str(number_of_threads) + """)"""

    return new_sdqlpy_info

def get_query_tables(query_path: str) -> list:
    query_content = None
    with open(query_path) as f:
        query_content = f.read()
    
    potential_tables = query_content.split("query_tables = [")[1].split("]")[0].split(",")
    for idx, tab in enumerate(potential_tables):
        potential_tables[idx] = tab.strip()
    
    if not all([x in TPCH_TABLES for x in potential_tables]):
        raise Exception("Unrecognised tables in query")
    
    return potential_tables

def check_sdqlpy_file(file_path: str):
    required_variables = ["iterations", "query_function", "query_tables", "query_columns", "data_write_path"]
    
    query_content = None
    with open(file_path) as f:
        query_content = f.read()
        
    for var in required_variables:
        if f"{var} =" not in query_content:
            print(f"Variable: {var} was not found in the query file")
            print(query_content)
            raise Exception("Query Error")

def run_sdqlpy(query_path, iterations, sdqlpy_setup, data_location, number_of_threads):
    # Prepend SDQLpy information
    query_tables = get_query_tables(query_path)
    sdqlpy_info = build_sdqlpy_info(data_location, query_tables, number_of_threads)

    line_prepender(query_path, "\n")
    line_prepender(query_path, sdqlpy_info)
    # Finish writing options out
    iterations = f"iterations = {iterations}"
    add_line_at_end(query_path, iterations)
    data_json_name = f"{''.join(random.sample(string.ascii_uppercase, 12))}.json"
    data_write_path = f'data_write_path = "{data_json_name}"'
    add_line_at_end(query_path, data_write_path)
    
    check_sdqlpy_file(query_path)
    
    bench_runner = "bench_runner(iterations, query_function, query_tables, query_columns, data_write_path)"
    add_line_at_end(query_path, "\n")
    add_line_at_end(query_path, bench_runner)
    add_line_at_end(query_path, "\n")
    
    # Move Query to 
    sdqlpy_queries_path = f"{sdqlpy_setup['Location']}/{sdqlpy_setup['Queries Directory']}"
    _, query_base = os.path.split(query_path)
    overall_query_path = f"{sdqlpy_queries_path}/{query_base}"
    shutil.copyfile(query_path, overall_query_path)
    
    # Run query
    _, bench_run_base = os.path.split(sdqlpy_setup['Bench Run'])
    command = f"./{sdqlpy_setup['Location']}/{bench_run_base} {sdqlpy_setup['Location']}/{sdqlpy_setup['Queries Directory']}/{query_base}"

    result = subprocess.run(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600)
    if result.returncode != 0:
        raise Exception("Error when running SDQLpy")
    
    data_json_path = f"{sdqlpy_setup['Location']}/{sdqlpy_setup['Queries Directory']}/{data_json_name}"
    
    with open(data_json_path) as f:
        query_results = json.load(f)
    
    # Remove data_json and query_file
    # TODO: Undo this!
    os.remove(data_json_path)
    # os.remove(overall_query_path)
    
    # Return Query results
    return query_results
