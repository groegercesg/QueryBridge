import duckdb

# connect to an in-memory database
con = duckdb.connect(database='duckdb_tpch.duckdb', read_only=False)

print("DuckDB Testing")

query_name = "q4"
query_file = "sql_to_pandas/queries/4.sql"

# TODO: Make columns lowercase

create_table_commands = [
        "CREATE TABLE PART(p_partkey INTEGER, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, p_type VARCHAR, p_size INTEGER, p_container VARCHAR, p_retailprice DECIMAL, p_comment VARCHAR);"
        "CREATE TABLE SUPPLIER(s_suppkey INTEGER, s_name VARCHAR, s_address VARCHAR, s_nationkey INTEGER, s_phone VARCHAR, s_acctbal DECIMAL, s_comment VARCHAR);"
        "CREATE TABLE PARTSUPP(ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER, ps_supplycost DECIMAL, ps_comment VARCHAR);"
        "CREATE TABLE CUSTOMER(c_custkey INTEGER, c_name VARCHAR, c_address VARCHAR, c_nationkey INTEGER, c_phone VARCHAR, c_acctbal DECIMAL, c_mktsegment VARCHAR, c_comment VARCHAR);"
        "CREATE TABLE ORDERS(o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus VARCHAR, o_totalprice DECIMAL, o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority INTEGER, o_comment VARCHAR);"
        "CREATE TABLE LINEITEM(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity DECIMAL, l_extendedprice DECIMAL, l_discount DECIMAL, l_tax DECIMAL, l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR);"
        "CREATE TABLE NATION(n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR);"
        "CREATE TABLE REGION(r_regionkey INTEGER, r_name VARCHAR, r_comment VARCHAR);"
    ]

load_data_commands = [
        "COPY PART FROM 'data_storage/part.tbl.csv' ( DELIMITER '|' );",
        "COPY SUPPLIER FROM 'data_storage/supplier.tbl.csv' ( DELIMITER '|' );",
        "COPY PARTSUPP FROM 'data_storage/partsupp.tbl.csv' ( DELIMITER '|' );",
        "COPY CUSTOMER FROM 'data_storage/customer.tbl.csv' ( DELIMITER '|' );",
        "COPY ORDERS FROM 'data_storage/orders.tbl.csv' ( DELIMITER '|' );",
        "COPY LINEITEM FROM 'data_storage/lineitem.tbl.csv' ( DELIMITER '|' );",
        "COPY NATION FROM 'data_storage/nation.tbl.csv' ( DELIMITER '|' );",
        "COPY REGION FROM 'data_storage/region.tbl.csv' ( DELIMITER '|' );",
    ]

# enable profiling in json format
# write the profiling output to a specific file on disk
# configure the system to use 1 thread
output_explain_name = str(query_name) + "_duck_db_explain.json"

explain_commands = ["PRAGMA enable_profiling='json';",
                    "PRAGMA profile_output='" + str(output_explain_name) + "';",
                    "PRAGMA explain_output='ALL';",
                    "SET threads TO 1;"]

for command in create_table_commands:
    con.execute(command)
    
for command in load_data_commands:
    con.execute(command)
    
for command in explain_commands:
    con.execute(command)

"""
query_file_data = None
with open(query_file, 'r') as f:
    query_file_data = f.read()

#explain_command = "EXPLAIN SELECT name FROM students JOIN exams USING (sid) WHERE name LIKE 'Ma%';"
explain_command = str(query_file_data)
# TODO: Get column names
results = con.execute(explain_command).fetchall()

print(results)
"""