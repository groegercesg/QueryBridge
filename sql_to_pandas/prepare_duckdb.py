import json
import duckdb
import os
import subprocess
from subprocess import DEVNULL, STDOUT, PIPE
import glob
import re
import argparse
import sys



class prep_duck():
    """
    Required methods:
        get_explain
        execute_query
        is_database_empty
        __init__
    """
    def __init__(self, in_duckdb, dbgen_path = None):
        # Read connection details from file to json
        self.connection = duckdb.connect(database=in_duckdb, read_only=False)
        
        # Set dbgen_path
        self.dbgen_path = dbgen_path
        
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