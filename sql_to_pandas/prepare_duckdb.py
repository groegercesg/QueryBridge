import json
import duckdb
import os

class prep_duck():
    """
    Required methods:
        get_explain
        execute_query
        is_database_empty
        __init__
    """
    def __init__(self, in_duckdb):
        self.file_name = str(in_duckdb)
        self.connection = duckdb.connect(database=in_duckdb, read_only=False)
        
        # Store TPC-H tables
        self.tables = ['LINEITEM', 'PARTSUPP', 'ORDERS', 'CUSTOMER', 'SUPPLIER', 'NATION', 'REGION', 'PART']
    
    def is_database_empty(self):        
        cursor_fetch = self.connection.execute("""SELECT table_name FROM information_schema.tables""").fetchall()
        
        if len(cursor_fetch) > 0:
            return False
        else:
            print(cursor_fetch)
            return True
        
    def execute_query(self, query):
        # Execute a query
        self.connection.execute(query)
             
    def get_explain(self, query, query_name=None):
        
        explain_opts = "EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON) "
        
        # Replace out our explain options
        if explain_opts in query:
            query = query.replace(explain_opts, "")
        
        if query_name == None:
            raise Exception("We haven't set our query name!")
        output_explain_name = str(query_name) + "_duck_db_explain.json"

        explain_commands = ["PRAGMA enable_profiling='json';",
                            "PRAGMA profile_output='" + str(output_explain_name) + "';",
                            "PRAGMA explain_output='ALL';",
                            "SET threads TO 1;"]
            
        for command in explain_commands:
            self.connection.execute(command)
        
        self.connection.execute(query).fetchall()
        
        # Read json and return it
        f = open(output_explain_name)
        json_data = json.load(f)
        
        # Delete the json file
        if os.path.exists(output_explain_name):
            os.remove(output_explain_name)
        
        return json_data
    
    def prepare_database(self, data_dir):
        # Set the data dir
        self.data_dir = data_dir
        
        # Remove existing file
        os.remove(self.file_name)
        
        # Create new file
        con = duckdb.connect(database=self.file_name, read_only=False)
        print("Created new Database file")
        
        # Create table
        create_table_commands = [
            "CREATE TABLE part(p_partkey INTEGER, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, p_type VARCHAR, p_size INTEGER, p_container VARCHAR, p_retailprice DECIMAL, p_comment VARCHAR);"
            "CREATE TABLE supplier(s_suppkey INTEGER, s_name VARCHAR, s_address VARCHAR, s_nationkey INTEGER, s_phone VARCHAR, s_acctbal DECIMAL, s_comment VARCHAR);"
            "CREATE TABLE partsupp(ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER, ps_supplycost DECIMAL, ps_comment VARCHAR);"
            "CREATE TABLE customer(c_custkey INTEGER, c_name VARCHAR, c_address VARCHAR, c_nationkey INTEGER, c_phone VARCHAR, c_acctbal DECIMAL, c_mktsegment VARCHAR, c_comment VARCHAR);"
            "CREATE TABLE orders(o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus VARCHAR, o_totalprice DECIMAL, o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority INTEGER, o_comment VARCHAR);"
            "CREATE TABLE lineitem(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity DECIMAL, l_extendedprice DECIMAL, l_discount DECIMAL, l_tax DECIMAL, l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR);"
            "CREATE TABLE nation(n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR);"
            "CREATE TABLE region(r_regionkey INTEGER, r_name VARCHAR, r_comment VARCHAR);"
        ]
        
        for command in create_table_commands:
            con.execute(command)
        
        print("Created tables")
        
        # Load data
        load_data_commands = [
            "COPY PART FROM '" + str(self.data_dir) + "/part.tbl.csv' ( DELIMITER '|' );",
            "COPY SUPPLIER FROM '" + str(self.data_dir) + "/supplier.tbl.csv' ( DELIMITER '|' );",
            "COPY PARTSUPP FROM '" + str(self.data_dir) + "/partsupp.tbl.csv' ( DELIMITER '|' );",
            "COPY CUSTOMER FROM '" + str(self.data_dir) + "/customer.tbl.csv' ( DELIMITER '|' );",
            "COPY ORDERS FROM '" + str(self.data_dir) + "/orders.tbl.csv' ( DELIMITER '|' );",
            "COPY LINEITEM FROM '" + str(self.data_dir) + "/lineitem.tbl.csv' ( DELIMITER '|' );",
            "COPY NATION FROM '" + str(self.data_dir) + "/nation.tbl.csv' ( DELIMITER '|' );",
            "COPY REGION FROM '" + str(self.data_dir) + "/region.tbl.csv' ( DELIMITER '|' );",
        ]
        
        for command in load_data_commands:
            con.execute(command)
            
        print("Loaded data into tables")
        
        # TODO: Set indexes
        
        
        print("DuckDB Database creation completed")
