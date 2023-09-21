from prepare_databases.prepare_database import PrepareDatabase
import duckdb
import json
import os

class PrepareDuckDB(PrepareDatabase):
    def __init__(self, connection_details):
        super().__init__(connection_details, "Duck DB")
        self.connection = self.__open_connection()
        self.explain_options = '\n'
        
    def is_database_empty(self):
        cursor_fetch = self.execute_query("""SELECT table_name FROM information_schema.tables""")
        
        if len(cursor_fetch) > 0:
            return False
        else:
            print(cursor_fetch)
            return True
        
    def execute_query(self, query_text):
        # Execute a query
        return self.connection.execute(query_text).fetchall()
        
    def get_explain(self, query_text, query_name=None):
        query_text = self.create_explain(query_text)
        if query_name == None:
            raise Exception("We haven't set our query name!")
        
        output_explain_name = str(query_name) + "_duck_db_explain.json"

        explain_commands = ["PRAGMA enable_profiling='json';",
                            "PRAGMA profile_output='" + str(output_explain_name) + "';",
                            "PRAGMA explain_output='ALL';",
                            "SET explain_output='all';",
                            "SET threads TO 1;"]
            
        for command in explain_commands:
            self.execute_query(command)
        
        try:
            self.execute_query(query_text)
        except RuntimeError as ex:
            if "Catalog Error:" == str(ex)[:14]:
                name = str(str(ex).split('name "')[1].split('"')[0]).strip()
                self.execute_query("drop view " + name + ";")
                self.execute_query(query_text)
            else:
                print(ex)
                exit(0)
        except Exception as ex_main:
            print(ex_main)
            exit(0)
        
        # Read json and return it
        f = open(output_explain_name)
        json_data = json.load(f)
        
        # Delete the json file
        if os.path.exists(output_explain_name):
            os.remove(output_explain_name)
        
        return json_data, query_text
    
    def __open_connection(self):
        try:
            connection = duckdb.connect(database=self.connection_details, read_only=False)
        except Exception as ex:
            raise Exception(ex)
        return connection
    
    def prepare_database(self, data_dir, constants_dir=None):
        # Set the data dir
        self.data_dir = data_dir

        # Close self connection
        self.connection.close()
        
        # Remove existing file
        try:
            os.remove(self.connection_details)
        except OSError:
            pass
        
        # Create new file
        self.connection = self.__open_connection()
        print("Created new Database file, at: " + str(self.connection_details))
        
        # Create table
        create_table_commands = [
            "CREATE TABLE region(r_regionkey INTEGER PRIMARY KEY, r_name VARCHAR, r_comment VARCHAR);",
            "CREATE TABLE nation(n_nationkey INTEGER PRIMARY KEY, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR);", # , FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey)
            "CREATE TABLE supplier(s_suppkey INTEGER PRIMARY KEY, s_name VARCHAR, s_address VARCHAR, s_nationkey INTEGER, s_phone VARCHAR, s_acctbal DECIMAL, s_comment VARCHAR);", # , FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)
            "CREATE TABLE customer(c_custkey INTEGER PRIMARY KEY, c_name VARCHAR, c_address VARCHAR, c_nationkey INTEGER, c_phone VARCHAR, c_acctbal DECIMAL, c_mktsegment VARCHAR, c_comment VARCHAR);", # , FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)
            "CREATE TABLE part(p_partkey INTEGER PRIMARY KEY, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, p_type VARCHAR, p_size INTEGER, p_container VARCHAR, p_retailprice DECIMAL, p_comment VARCHAR);",
            "CREATE TABLE partsupp(ps_partkey INTEGER, ps_suppkey INTEGER, PRIMARY KEY(ps_partkey, ps_suppkey), ps_availqty INTEGER, ps_supplycost DECIMAL, ps_comment VARCHAR);", # , FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey), FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey)
            "CREATE TABLE orders(o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_orderstatus VARCHAR, o_totalprice DECIMAL, o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority INTEGER, o_comment VARCHAR);", # , FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
            "CREATE TABLE lineitem(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, PRIMARY KEY(l_orderkey, l_linenumber), l_quantity DECIMAL, l_extendedprice DECIMAL, l_discount DECIMAL, l_tax DECIMAL, l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR);" # , FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey) ON DELETE CASCADE, FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey,ps_suppkey)
        ]
        
        for command in create_table_commands:
            self.execute_query(command)
        
        print("Created tables")
        
        # Load data
        load_data_commands = [
            "COPY part FROM '" + str(self.data_dir) + "/part.tbl.csv' ( DELIMITER '|' );",
            "COPY supplier FROM '" + str(self.data_dir) + "/supplier.tbl.csv' ( DELIMITER '|' );",
            "COPY partsupp FROM '" + str(self.data_dir) + "/partsupp.tbl.csv' ( DELIMITER '|' );",
            "COPY customer FROM '" + str(self.data_dir) + "/customer.tbl.csv' ( DELIMITER '|' );",
            "COPY orders FROM '" + str(self.data_dir) + "/orders.tbl.csv' ( DELIMITER '|' );",
            "COPY lineitem FROM '" + str(self.data_dir) + "/lineitem.tbl.csv' ( DELIMITER '|' );",
            "COPY nation FROM '" + str(self.data_dir) + "/nation.tbl.csv' ( DELIMITER '|' );",
            "COPY region FROM '" + str(self.data_dir) + "/region.tbl.csv' ( DELIMITER '|' );",
        ]
        
        for command in load_data_commands:
            self.execute_query(command)
            
        print("Loaded data into tables")
        
        # TODO: Set indexes
        """
        
        for command in primary_key_commands:
            con.execute(command)
        
        foreign_key_commands = [
            "ALTER TABLE supplier ADD FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey);",
            "ALTER TABLE partsupp ADD FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey);",
            "ALTER TABLE partsupp ADD FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey);",
            "ALTER TABLE customer ADD FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey);",
            "ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey);",
            "ALTER TABLE lineitem ADD FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey) ON DELETE CASCADE;",
            "ALTER TABLE lineitem ADD FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey,ps_suppkey);",
            "ALTER TABLE nation ADD FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey);"
        ]
        
        for command in foreign_key_commands:
            con.execute(command)
            
        index_on_f_keys_commands = [
            "CREATE INDEX idx_supplier_nation_key ON supplier (s_nationkey);",
            "CREATE INDEX idx_partsupp_partkey ON partsupp (ps_partkey);",
            "CREATE INDEX idx_partsupp_suppkey ON partsupp (ps_suppkey);",
            "CREATE INDEX idx_customer_nationkey ON customer (c_nationkey);",
            "CREATE INDEX idx_orders_custkey ON orders (o_custkey);",
            "CREATE INDEX idx_lineitem_orderkey ON lineitem (l_orderkey);",
            "CREATE INDEX idx_lineitem_part_supp ON lineitem (l_partkey, l_suppkey);",
            "CREATE INDEX idx_nation_regionkey ON nation (n_regionkey);"
        ]
        
        for command in index_on_f_keys_commands:
            con.execute(command)
        """
        
        # Commit changes performed within a transaction
        self.connection.commit()
        
        print("Indexes and Foreign keys set on tables")
        
        print("DuckDB Database creation completed")
        