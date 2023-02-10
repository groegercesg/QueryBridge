import duckdb

# connect to an in-memory database
con = duckdb.connect()

print("DuckDB Testing")

query_name = "q4"
query_file = "sql_to_pandas/queries/4.sql"

# TODO: Make columns lowercase

create_table_commands = [
        "CREATE TABLE PART(P_PARTKEY INTEGER, P_NAME VARCHAR, P_MFGR VARCHAR, P_BRAND VARCHAR, P_TYPE VARCHAR, P_SIZE INTEGER, P_CONTAINER VARCHAR, P_RETAILPRICE DECIMAL, P_COMMENT VARCHAR);"
        "CREATE TABLE SUPPLIER(S_SUPPKEY INTEGER, S_NAME VARCHAR, S_ADDRESS VARCHAR, S_NATIONKEY INTEGER, S_PHONE VARCHAR, S_ACCTBAL DECIMAL, S_COMMENT VARCHAR);"
        "CREATE TABLE PARTSUPP(PS_PARTKEY INTEGER, PS_SUPPKEY INTEGER, PS_AVAILQTY INTEGER, PS_SUPPLYCOST DECIMAL, PS_COMMENT VARCHAR);"
        "CREATE TABLE CUSTOMER(C_CUSTKEY INTEGER, C_NAME VARCHAR, C_ADDRESS VARCHAR, C_NATIONKEY INTEGER, C_PHONE VARCHAR, C_ACCTBAL DECIMAL, C_MKTSEGMENT VARCHAR, C_COMMENT VARCHAR);"
        "CREATE TABLE ORDERS(O_ORDERKEY INTEGER, O_CUSTKEY INTEGER, O_ORDERSTATUS VARCHAR, O_TOTALPRICE DECIMAL, O_ORDERDATE DATE, O_ORDERPRIORITY VARCHAR, O_CLERK VARCHAR, O_SHIPPRIORITY INTEGER, O_COMMENT VARCHAR);"
        "CREATE TABLE LINEITEM(L_ORDERKEY INTEGER, L_PARTKEY INTEGER, L_SUPPKEY INTEGER, L_LINENUMBER INTEGER, L_QUANTITY DECIMAL, L_EXTENDEDPRICE DECIMAL, L_DISCOUNT DECIMAL, L_TAX DECIMAL, L_RETURNFLAG VARCHAR, L_LINESTATUS VARCHAR, L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT VARCHAR, L_SHIPMODE VARCHAR, L_COMMENT VARCHAR);"
        "CREATE TABLE NATION(N_NATIONKEY INTEGER, N_NAME VARCHAR, N_REGIONKEY INTEGER, N_COMMENT VARCHAR);"
        "CREATE TABLE REGION(R_REGIONKEY INTEGER, R_NAME VARCHAR, R_COMMENT VARCHAR);"
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
    
query_file_data = None
with open(query_file, 'r') as f:
    query_file_data = f.read()

#explain_command = "EXPLAIN SELECT name FROM students JOIN exams USING (sid) WHERE name LIKE 'Ma%';"
explain_command = str(query_file_data)
# TODO: Get column names
results = con.execute(explain_command).fetchall()

print(results)